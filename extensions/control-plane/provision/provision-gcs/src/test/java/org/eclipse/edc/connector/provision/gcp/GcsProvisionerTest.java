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
 *       Google LLC - Initial implementation
 *
 */

package org.eclipse.edc.connector.provision.gcp;

import org.eclipse.edc.gcp.common.GcpAccessToken;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.gcp.common.GcpServiceAccount;
import org.eclipse.edc.gcp.common.GcsBucket;
import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.gcp.storage.StorageService;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;


class GcsProvisionerTest {

    private GcsProvisioner provisioner;
    private StorageService storageServiceMock;
    private IamService iamServiceMock;
    private Policy testPolicy;
    private GcpConfiguration gcpConfiguration;

    @BeforeEach
    void setUp() {
        storageServiceMock = mock();
        iamServiceMock = mock();
        gcpConfiguration = mock();
        testPolicy = Policy.Builder.newInstance().build();
        provisioner = new GcsProvisioner(gcpConfiguration, mock(Monitor.class), storageServiceMock, iamServiceMock);
    }

    @Test
    void canProvisionGcsResource() {
        var gcsResource = GcsResourceDefinition.Builder.newInstance()
                .id("TEST").location("TEST").storageClass("TEST")
                .build();
        assertThat(provisioner.canProvision(gcsResource)).isTrue();
    }

    @Test
    void provisionSuccess() {
        var resourceDefinitionId = "id";
        var location = "location";
        var storageClass = "storage-class";
        var transferProcessId = UUID.randomUUID().toString();
        var resourceDefinition = createResourceDefinition(resourceDefinitionId, location,
                storageClass, transferProcessId);
        var bucketName = resourceDefinition.getId();
        var bucketLocation = resourceDefinition.getLocation();

        var bucket = new GcsBucket(bucketName);
        var serviceAccount = IamService.ADC_SERVICE_ACCOUNT;
        var token = new GcpAccessToken("token", 123);

        when(gcpConfiguration.serviceAccountName()).thenReturn(null);

        when(storageServiceMock.getOrCreateBucket(bucketName, bucketLocation)).thenReturn(bucket);
        when(storageServiceMock.isEmpty(bucketName)).thenReturn(true);
        when(iamServiceMock.getServiceAccount(null)).thenReturn(IamService.ADC_SERVICE_ACCOUNT);
        when(iamServiceMock.createAccessToken(IamService.ADC_SERVICE_ACCOUNT, "https://www.googleapis.com/auth/devstorage.read_write")).thenReturn(token);
        doNothing().when(storageServiceMock).addProviderPermissions(bucket, serviceAccount);

        var response = provisioner.provision(resourceDefinition, testPolicy).join().getContent();

        verify(storageServiceMock).getOrCreateBucket(bucketName, bucketLocation);
        verify(iamServiceMock).getServiceAccount(null);
        verify(iamServiceMock).createAccessToken(IamService.ADC_SERVICE_ACCOUNT, "https://www.googleapis.com/auth/devstorage.read_write");

        assertThat(response.getResource()).isInstanceOfSatisfying(GcsProvisionedResource.class, resource -> {
            assertThat(resource.getId()).isEqualTo(resourceDefinitionId);
            assertThat(resource.getTransferProcessId()).isEqualTo(transferProcessId);
            assertThat(resource.getLocation()).isEqualTo(location);
            assertThat(resource.getStorageClass()).isEqualTo(storageClass);
        });
        assertThat(response.getSecretToken()).isInstanceOfSatisfying(GcpAccessToken.class, secretToken -> {
            assertThat(secretToken.getToken()).isEqualTo("token");
        });
    }

    @Test
    void provisionWithImpersonationSuccess() {
        var resourceDefinitionId = "id";
        var location = "location";
        var storageClass = "storage-class";
        var serviceAccount = new GcpServiceAccount("test-sa", "sa-name", "description");
        var token = new GcpAccessToken("token", 123);
        var transferProcessId = UUID.randomUUID().toString();
        var resourceDefinition = createResourceDefinition(resourceDefinitionId, location,
                storageClass, transferProcessId, serviceAccount.getName());
        var bucketName = resourceDefinition.getId();
        var bucket = new GcsBucket(bucketName);
        var bucketLocation = resourceDefinition.getLocation();

        when(gcpConfiguration.serviceAccountName()).thenReturn(serviceAccount.getName());

        when(storageServiceMock.getOrCreateBucket(bucketName, bucketLocation)).thenReturn(bucket);
        when(storageServiceMock.isEmpty(bucketName)).thenReturn(true);
        when(iamServiceMock.getServiceAccount(serviceAccount.getName())).thenReturn(serviceAccount);
        when(iamServiceMock.createAccessToken(serviceAccount, "https://www.googleapis.com/auth/devstorage.read_write")).thenReturn(token);
        doNothing().when(storageServiceMock).addProviderPermissions(bucket, serviceAccount);

        var response = provisioner.provision(resourceDefinition, testPolicy).join().getContent();

        assertThat(response.getResource()).isInstanceOfSatisfying(GcsProvisionedResource.class, resource -> {
            assertThat(resource.getId()).isEqualTo(resourceDefinitionId);
            assertThat(resource.getTransferProcessId()).isEqualTo(transferProcessId);
            assertThat(resource.getLocation()).isEqualTo(location);
            assertThat(resource.getStorageClass()).isEqualTo(storageClass);
        });
        assertThat(response.getSecretToken()).isInstanceOfSatisfying(GcpAccessToken.class, secretToken -> {
            assertThat(secretToken.getToken()).isEqualTo("token");
        });

        verify(storageServiceMock).getOrCreateBucket(bucketName, bucketLocation);
        verify(iamServiceMock).createAccessToken(any(), eq("https://www.googleapis.com/auth/devstorage.read_write"));
    }

    @Test
    void provisionSucceedsIfBucketNotEmpty() {
        var resourceDefinition = createResourceDefinition();
        var bucketName = resourceDefinition.getId();
        var bucketLocation = resourceDefinition.getLocation();

        when(iamServiceMock.getServiceAccount(null)).thenReturn(IamService.ADC_SERVICE_ACCOUNT);
        when(gcpConfiguration.serviceAccountName()).thenReturn(null);
        when(storageServiceMock.getOrCreateBucket(bucketName, bucketLocation)).thenReturn(new GcsBucket(bucketName));
        when(storageServiceMock.isEmpty(bucketName)).thenReturn(false);

        var response = provisioner.provision(resourceDefinition, testPolicy).join();

        assertThat(response.failed()).isFalse();

        verify(storageServiceMock).getOrCreateBucket(bucketName, bucketLocation);
        verify(iamServiceMock).createAccessToken(IamService.ADC_SERVICE_ACCOUNT, "https://www.googleapis.com/auth/devstorage.read_write");
    }

    @Test
    void provisionFailsBecauseOfApiError() {
        var resourceDefinition = createResourceDefinition();
        var bucketName = resourceDefinition.getId();
        var bucketLocation = resourceDefinition.getLocation();

        doThrow(new GcpException("some error")).when(storageServiceMock).getOrCreateBucket(bucketName, bucketLocation);

        var response = provisioner.provision(resourceDefinition, testPolicy).join();
        assertThat(response.failed()).isTrue();
    }

    @Test
    void canDeprovisionGcsResource() {
        var gcsProvisionedResource = GcsProvisionedResource.Builder.newInstance().id("TEST")
                .transferProcessId("TEST").resourceDefinitionId("TEST").resourceName("TEST").build();
        assertThat(provisioner.canDeprovision(gcsProvisionedResource)).isTrue();
    }

    @Test
    void deprovisionSuccess() {
        var email = "test-email";
        var name = "test-name";
        var id = "test-id";
        var description = "sa-description";
        var serviceAccount = new GcpServiceAccount(email, name, description);

        var resource = createGcsProvisionedResource(email, name, id);

        var response = provisioner.deprovision(resource, testPolicy).join().getContent();
        assertThat(response.getProvisionedResourceId()).isEqualTo(id);
    }

    private GcsProvisionedResource createGcsProvisionedResource(String serviceAccountEmail, String serviceAccountName, String id) {
        return GcsProvisionedResource.Builder.newInstance().resourceName("name")
                .id(id)
                .resourceDefinitionId(id)
                .bucketName("bucket")
                .location("location")
                .storageClass("standard")
                .transferProcessId("transfer-id")
                .serviceAccountName(serviceAccountName)
                .serviceAccountEmail(serviceAccountEmail)
                .build();
    }

    private GcsResourceDefinition createResourceDefinition() {
        return createResourceDefinition("id", "location",
                "storage-class", "transfer-id");
    }

    private GcsResourceDefinition createResourceDefinition(String id, String location, String storageClass, String transferProcessId) {
        return GcsResourceDefinition.Builder.newInstance().id(id)
                .location(location).storageClass(storageClass)
                .transferProcessId(transferProcessId).build();
    }

    private GcsResourceDefinition createResourceDefinition(String id, String location, String storageClass, String transferProcessId, String serviceAccountName) {
        return GcsResourceDefinition.Builder.newInstance().id(id)
            .location(location).storageClass(storageClass)
            .transferProcessId(transferProcessId)
            .serviceAccountName(serviceAccountName)
            .build();
    }

    private ArgumentMatcher<GcpServiceAccount> matches(GcpServiceAccount serviceAccount) {
        return argument -> argument.getEmail().equals(serviceAccount.getEmail()) && argument.getName().equals(serviceAccount.getName());
    }
}
