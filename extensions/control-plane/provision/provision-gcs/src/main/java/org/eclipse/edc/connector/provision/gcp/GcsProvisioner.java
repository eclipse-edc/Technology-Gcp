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

import com.google.common.collect.ImmutableList;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.Provisioner;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.DeprovisionedResource;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ProvisionResponse;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.gcp.common.GcpServiceAccount;
import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.gcp.storage.StorageService;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.ResponseStatus;
import org.eclipse.edc.spi.response.StatusResult;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class GcsProvisioner implements Provisioner<GcsResourceDefinition, GcsProvisionedResource> {
    private static final ImmutableList<String> OAUTH_SCOPE = ImmutableList.of("https://www.googleapis.com/auth/cloud-platform");
    private static final long ONE_HOUR_IN_S = TimeUnit.HOURS.toSeconds(1);
    private final Monitor monitor;
    private final StorageService storageService;
    private final IamService iamService;
    private final GcpConfiguration gcpConfiguration;

    public GcsProvisioner(GcpConfiguration gcpConfiguration, Monitor monitor, StorageService storageService, IamService iamService) {
        this.monitor = monitor;
        this.storageService = storageService;
        this.iamService = iamService;
        this.gcpConfiguration = gcpConfiguration;
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
        var bucketName = Optional.ofNullable(resourceDefinition.getBucketName())
                .orElseGet(() -> {
                    var generatedBucketName = resourceDefinition.getId();
                    monitor.debug("GCS bucket name generated: " + generatedBucketName);
                    return generatedBucketName;
                });

        monitor.debug("GCS Bucket request submitted: " + bucketName);

        var bucketLocation = resourceDefinition.getLocation();
        var resourceName = bucketName + "-bucket";
        try {
            var bucket = storageService.getOrCreateBucket(bucketName, bucketLocation);
            var serviceAccount = iamService.getServiceAccount(resourceDefinition.getServiceAccountName());
            var token = iamService.createAccessToken(serviceAccount);
            var resource = getProvisionedResource(resourceDefinition, resourceName, bucketName, serviceAccount);
            var response = ProvisionResponse.Builder.newInstance().resource(resource).secretToken(token).build();

            return CompletableFuture.completedFuture(StatusResult.success(response));
        } catch (GcpException gcpException) {
            return completedFuture(StatusResult.failure(ResponseStatus.FATAL_ERROR, gcpException.toString()));
        }
    }

    @Override
    public CompletableFuture<StatusResult<DeprovisionedResource>> deprovision(
            GcsProvisionedResource provisionedResource, Policy policy) {
        return CompletableFuture.completedFuture(StatusResult.success(
                DeprovisionedResource.Builder.newInstance()
                        .provisionedResourceId(provisionedResource.getId()).build()));
    }

    private GcsProvisionedResource getProvisionedResource(GcsResourceDefinition resourceDefinition, String resourceName, String bucketName, GcpServiceAccount serviceAccount) {
        String serviceAccountEmail = null;
        String serviceAccountName = null;
        if (serviceAccount != null) {
            serviceAccountEmail = serviceAccount.getEmail();
            serviceAccountName = serviceAccount.getName();
        }
        return GcsProvisionedResource.Builder.newInstance()
                .id(resourceDefinition.getId())
                .resourceDefinitionId(resourceDefinition.getId())
                .location(resourceDefinition.getLocation())
                .storageClass(resourceDefinition.getStorageClass())
                .serviceAccountEmail(serviceAccountEmail)
                .serviceAccountName(serviceAccountName)
                .transferProcessId(resourceDefinition.getTransferProcessId())
                .resourceName(resourceName)
                .bucketName(bucketName)
                .hasToken(true).build();
    }
}
