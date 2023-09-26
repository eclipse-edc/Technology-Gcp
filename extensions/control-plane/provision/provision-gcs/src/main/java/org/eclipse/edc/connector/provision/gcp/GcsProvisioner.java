/*
 *  Copyright (c) 2022 Google LLC
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Google LCC - Initial implementation
 *
 */

package org.eclipse.edc.connector.provision.gcp;

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.iam.admin.v1.IAMClient;
import com.google.cloud.iam.admin.v1.IAMSettings;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.iam.credentials.v1.IamCredentialsSettings;
import com.google.cloud.iam.credentials.v1.stub.IamCredentialsStubSettings;
import com.google.cloud.storage.StorageOptions;
import org.eclipse.edc.connector.transfer.spi.provision.Provisioner;
import org.eclipse.edc.connector.transfer.spi.types.DeprovisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionResponse;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.gcp.common.GcpCredentials;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.gcp.common.GcpServiceAccount;
import org.eclipse.edc.gcp.common.GcpServiceAccountCredentials;
import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.gcp.iam.IamServiceImpl;
import org.eclipse.edc.gcp.storage.GcsStoreSchema;
import org.eclipse.edc.gcp.storage.StorageService;
import org.eclipse.edc.gcp.storage.StorageServiceImpl;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.ResponseStatus;
import org.eclipse.edc.spi.response.StatusResult;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class GcsProvisioner implements Provisioner<GcsResourceDefinition, GcsProvisionedResource> {

    private final Monitor monitor;
    private final GcpCredentials gcpCredential;
    private final @Nullable String projectId;

    public GcsProvisioner(Monitor monitor, GcpCredentials gcpCredential, @Nullable String projectId) {
        this.monitor = monitor;
        this.gcpCredential = gcpCredential;
        this.projectId = projectId;
    }

    @Override
    public boolean canProvision(ResourceDefinition resourceDefinition) {
        return resourceDefinition instanceof GcsResourceDefinition;
    }

    @Override
    public boolean canDeprovision(ProvisionedResource resourceDefinition) {
        return resourceDefinition instanceof GcsProvisionedResource;
    }

    @Override
    public CompletableFuture<StatusResult<ProvisionResponse>> provision(
            GcsResourceDefinition resourceDefinition, Policy policy) {

        var projectId = getProjectId(resourceDefinition.getProjectId());
        var tokenKeyName = resourceDefinition.getTokenKeyName();
        var serviceAccountKeyName = resourceDefinition.getServiceAccountKeyName();
        var serviceAccountKeyValue = resourceDefinition.getServiceAccountKeyValue();
        var gcpServiceAccountCredentials = new GcpServiceAccountCredentials(tokenKeyName, serviceAccountKeyName, serviceAccountKeyValue);
        var googleCredentials = gcpCredential.resolveGoogleCredentialsFromDataAddress(gcpServiceAccountCredentials);
        var iamService = createIamService(monitor,
                projectId, googleCredentials);


        var storageClient = StorageOptions.newBuilder()
                .setCredentials(googleCredentials)
                .setProjectId(projectId).build().getService();
        var storageService = new StorageServiceImpl(storageClient, monitor);
        return provision(resourceDefinition, iamService, storageService);
    }

    public CompletableFuture<StatusResult<ProvisionResponse>> provision(
            GcsResourceDefinition resourceDefinition,
            IamService iamService, StorageService storageService) {
        var bucketName = Optional.ofNullable(resourceDefinition.getBucketName())
                .orElseGet(() -> {
                    var generatedBucketName = resourceDefinition.getId();
                    monitor.debug("GCS bucket name generated: " + generatedBucketName);
                    return generatedBucketName;
                });

        monitor.debug("GCS Bucket request submitted: " + bucketName);

        var bucketLocation = resourceDefinition.getLocation();
        var resourceName = bucketName + "-bucket";
        var processId = resourceDefinition.getTransferProcessId();
        try {
            var bucket = storageService.getOrCreateBucket(bucketName, bucketLocation);


            // TODO avoid creating / leaking service account at the end of the process
            var serviceAccount = createServiceAccount(processId, bucketName, iamService);
            storageService.addProviderPermissions(bucket, serviceAccount);
            var token = iamService.createAccessToken(serviceAccount);

            var resource = getProvisionedResource(resourceDefinition, resourceName, bucketName, serviceAccount);

            var response = ProvisionResponse.Builder.newInstance().resource(resource).secretToken(token).build();
            return CompletableFuture.completedFuture(StatusResult.success(response));
        } catch (GcpException e) {
            return completedFuture(StatusResult.failure(ResponseStatus.FATAL_ERROR, e.toString()));
        }
    }

    /**
     * Supplier of {@link IAMClient} using application default credentials
     */
    private Supplier<IAMClient> getIamClientSupplier(GoogleCredentials googleCredentials) {
        return () -> {
            try {
                var iamSetting = IAMSettings.newBuilder()
                        .setCredentialsProvider(FixedCredentialsProvider.create(googleCredentials))
                        .build();
                return IAMClient.create(iamSetting);
            } catch (IOException e) {
                throw new GcpException("Error while creating IAMClient", e);
            }
        };
    }

    /**
     * Supplier of {@link IamCredentialsClient} using application default credentials
     */
    private Supplier<IamCredentialsClient> getIamCredentialsClientSupplier(GoogleCredentials googleCredentials) {

        return () -> {
            try {
                var iamCredentialsStubSettings = IamCredentialsStubSettings.newBuilder()
                        .setCredentialsProvider(FixedCredentialsProvider.create(googleCredentials))
                        .build();
                return IamCredentialsClient.create(IamCredentialsSettings.create(iamCredentialsStubSettings));
            } catch (IOException e) {
                throw new GcpException("Error while creating IamCredentialsClient", e);
            }
        };
    }


    @Override
    public CompletableFuture<StatusResult<DeprovisionedResource>> deprovision(
            GcsProvisionedResource provisionedResource, Policy policy) {
        IamService iamService;
        var projectId = getProjectId(provisionedResource.getProjectId());
        var dataAddress = provisionedResource.getDataAddress();
        String serviceAccountKeyName = dataAddress.getStringProperty(GcsStoreSchema.SERVICE_ACCOUNT_KEY_NAME);
        String serviceAccountKeyValue = dataAddress.getStringProperty(GcsStoreSchema.SERVICE_ACCOUNT_KEY_VALUE);
        if (serviceAccountKeyName != null && serviceAccountKeyValue != null) {
            var gcpServiceAccountCredentials = new GcpServiceAccountCredentials(dataAddress.getKeyName(), serviceAccountKeyName, serviceAccountKeyValue);
            var googleCredentials = gcpCredential.resolveGoogleCredentialsFromDataAddress(gcpServiceAccountCredentials);

            iamService = createIamService(monitor, projectId, googleCredentials);
        } else {
            iamService = createIamService(monitor, projectId);
        }
        return deprovision(provisionedResource, iamService);
    }

    public CompletableFuture<StatusResult<DeprovisionedResource>> deprovision(GcsProvisionedResource provisionedResource, IamService iamService) {
        try {
            iamService.deleteServiceAccountIfExists(
                    new GcpServiceAccount(provisionedResource.getServiceAccountEmail(),
                            provisionedResource.getServiceAccountName(), ""));
        } catch (GcpException e) {
            return completedFuture(StatusResult.failure(ResponseStatus.FATAL_ERROR,
                    String.format("Deprovision failed with: %s", e.getMessage())));
        }
        return CompletableFuture.completedFuture(StatusResult.success(
                DeprovisionedResource.Builder.newInstance()
                        .provisionedResourceId(provisionedResource.getId()).build()));

    }

    private String getProjectId(@Nullable String projectId) {
        if (projectId != null && !projectId.isEmpty()) {
            return projectId;
        } else if (this.projectId != null && !this.projectId.isEmpty()) {
            return this.projectId;
        } else {
            throw new GcpException("ProjectId is empty. It must be provided either in the configs or asset properties!");
        }

    }

    private GcpServiceAccount createServiceAccount(String processId, String bucketName, IamService iamService) {
        var serviceAccountName = sanitizeServiceAccountName(processId);
        var uniqueServiceAccountDescription = generateUniqueServiceAccountDescription(processId, bucketName);
        return iamService.getOrCreateServiceAccount(serviceAccountName, uniqueServiceAccountDescription);
    }

    @NotNull
    private String sanitizeServiceAccountName(String processId) {
        // service account ID must be between 6 and 30 characters and can contain lowercase alphanumeric characters and dashes
        String processIdWithoutConstantChars = processId.replace("-", "");
        var maxAllowedSubstringLength = Math.min(26, processIdWithoutConstantChars.length());
        var uniqueId = processIdWithoutConstantChars.substring(0, maxAllowedSubstringLength);
        return "edc-" + uniqueId;
    }

    @NotNull
    private String generateUniqueServiceAccountDescription(String transferProcessId, String bucketName) {
        return String.format("transferProcess:%s\nbucket:%s", transferProcessId, bucketName);
    }

    private GcsProvisionedResource getProvisionedResource(GcsResourceDefinition resourceDefinition, String resourceName, String bucketName, GcpServiceAccount serviceAccount) {
        String serviceAccountEmail = null;
        String serviceAccountName = null;
        if (serviceAccount != null) {
            serviceAccountEmail = serviceAccount.getEmail();
            serviceAccountName = serviceAccount.getEmail();
        }
        return GcsProvisionedResource.Builder.newInstance()
                .id(resourceDefinition.getId())
                .resourceDefinitionId(resourceDefinition.getId())
                .location(resourceDefinition.getLocation())
                .projectId(resourceDefinition.getProjectId())
                .storageClass(resourceDefinition.getStorageClass())
                .serviceAccountEmail(serviceAccountEmail)
                .serviceAccountName(serviceAccountName)
                .transferProcessId(resourceDefinition.getTransferProcessId())
                .resourceName(resourceName)
                .bucketName(bucketName)
                .hasToken(true).build();
    }

    private IamService createIamService(Monitor monitor, String projectId, GoogleCredentials googleCredentials) {
        return IamServiceImpl.Builder.newInstance(monitor, projectId)
                .iamClientSupplier(getIamClientSupplier(googleCredentials))
                .iamCredentialsClientSupplier(getIamCredentialsClientSupplier(googleCredentials))
                .build();
    }

    private IamService createIamService(Monitor monitor, String projectId) {
        return IamServiceImpl.Builder.newInstance(monitor, projectId).build();
    }
}
