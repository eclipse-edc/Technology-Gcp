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

import org.eclipse.edc.connector.transfer.spi.provision.Provisioner;
import org.eclipse.edc.connector.transfer.spi.types.DeprovisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionResponse;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.gcp.common.GcpAccessToken;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.gcp.common.GcpServiceAccount;
import org.eclipse.edc.gcp.common.GcsBucket;
import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.gcp.storage.StorageService;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.ResponseStatus;
import org.eclipse.edc.spi.response.StatusResult;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class GcsProvisioner implements Provisioner<GcsResourceDefinition, GcsProvisionedResource> {

    private final Monitor monitor;
    private final StorageService storageService;
    private final IamService iamService;

    public GcsProvisioner(Monitor monitor, StorageService storageService, IamService iamService) {
        this.monitor = monitor;
        this.storageService = storageService;
        this.iamService = iamService;
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
        var bucketName = resourceDefinition.getId();
        var bucketLocation = resourceDefinition.getLocation();

        monitor.debug("GCS Bucket request submitted: " + bucketName);

        var resourceName = resourceDefinition.getId() + "-bucket";
        var processId = resourceDefinition.getTransferProcessId();
        try {
            var bucket = storageService.getOrCreateEmptyBucket(bucketName, bucketLocation);
            if (!storageService.isEmpty(bucketName)) {
                return completedFuture(StatusResult.failure(ResponseStatus.FATAL_ERROR, String.format("Bucket: %s already exists and is not empty.", bucketName)));
            }
            var serviceAccount = createServiceAccount(processId, bucketName);
            var token = createBucketAccessToken(bucket, serviceAccount);

            var resource = getProvisionedResource(resourceDefinition, resourceName, bucketName, serviceAccount);

            var response = ProvisionResponse.Builder.newInstance().resource(resource).secretToken(token).build();
            return CompletableFuture.completedFuture(StatusResult.success(response));
        } catch (GcpException e) {
            return completedFuture(StatusResult.failure(ResponseStatus.FATAL_ERROR, e.toString()));
        }
    }

    @Override
    public CompletableFuture<StatusResult<DeprovisionedResource>> deprovision(
            GcsProvisionedResource provisionedResource, Policy policy) {
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

    private GcpServiceAccount createServiceAccount(String processId, String buckedName) {
        var serviceAccountName = sanitizeServiceAccountName(processId);
        var uniqueServiceAccountDescription = generateUniqueServiceAccountDescription(processId, buckedName);
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

    private GcpAccessToken createBucketAccessToken(GcsBucket bucket, GcpServiceAccount serviceAccount) {
        storageService.addProviderPermissions(bucket, serviceAccount);
        return iamService.createAccessToken(serviceAccount);
    }

    private GcsProvisionedResource getProvisionedResource(GcsResourceDefinition resourceDefinition, String resourceName, String bucketName, GcpServiceAccount serviceAccount) {
        return GcsProvisionedResource.Builder.newInstance()
                .id(resourceDefinition.getId())
                .resourceDefinitionId(resourceDefinition.getId())
                .location(resourceDefinition.getLocation())
                .storageClass(resourceDefinition.getStorageClass())
                .serviceAccountEmail(serviceAccount.getEmail())
                .serviceAccountName(serviceAccount.getName())
                .transferProcessId(resourceDefinition.getTransferProcessId())
                .resourceName(resourceName)
                .bucketName(bucketName)
                .hasToken(true).build();
    }
}
