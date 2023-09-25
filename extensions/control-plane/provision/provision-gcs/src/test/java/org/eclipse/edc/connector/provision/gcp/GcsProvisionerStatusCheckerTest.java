/*
 *  Copyright (c) 2023 Google LLC
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

import com.google.api.gax.paging.Page;
import com.google.cloud.storage.Blob;
import org.eclipse.edc.connector.transfer.spi.types.DataRequest;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.TransferProcess;
import org.eclipse.edc.gcp.storage.GcsStoreSchema;
import org.eclipse.edc.gcp.storage.StorageService;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GcsProvisionerStatusCheckerTest {

    private GcsProvisionerStatusChecker statusChecker;
    private StorageService storageServiceMock;

    class ListPage implements Page<Blob> {
        private List<Blob> blobs;

        ListPage(List<Blob> blobs) {
            this.blobs = blobs;
        }

        public boolean hasNextPage() {
            return false;
        }

        public String getNextPageToken() {
            return "";
        }

        public Page<Blob> getNextPage() {
            return null;
        }

        public Iterable<Blob> iterateAll() {
            return blobs;
        }

        public Iterable<Blob> getValues() {
            return null;
        }
    }

    @BeforeEach
    void setUp() {
        storageServiceMock = mock(StorageService.class);
        statusChecker = new GcsProvisionerStatusChecker(storageServiceMock);
    }

    private List<ProvisionedResource> createResourceList(List<String> bucketNames) {
        List<ProvisionedResource> resources = new ArrayList<>();
        for (var bucketName : bucketNames) {
            GcsProvisionedResource resource = GcsProvisionedResource.Builder.newInstance()
                    .bucketName(bucketName)
                    .id("test_id")
                    .transferProcessId("test_transfer_process_id")
                    .resourceDefinitionId("test_resource_definition_id")
                    .resourceName("test_resource_name")
                    .dataAddress(DataAddress.Builder.newInstance().type(GcsStoreSchema.TYPE).build())
                    .build();
            resources.add(resource);
        }
        return resources;
    }

    private TransferProcess createTransferProcess(String bucketName) {
        TransferProcess transferProcess = TransferProcess.Builder.newInstance()
                .dataRequest(DataRequest.Builder.newInstance()
                        .dataDestination(DataAddress.Builder.newInstance()
                                .type(GcsStoreSchema.TYPE)
                                .property(GcsStoreSchema.BUCKET_NAME, bucketName).build())
                        .build()
                ).build();
        return transferProcess;
    }

    private String getValidTransferCompleteBlobName(String bucketName) {
        return bucketName + ".complete";
    }

    private String getInvalidTransferCompleteBlobName(String bucketName) {
        return bucketName + ".invalidcomplete";
    }

    private List<Blob> createValidBlobList(String bucketName) {
        List<Blob> blobs = new ArrayList<>();
        Blob blob = mock(Blob.class);
        when(blob.getName()).thenReturn(getValidTransferCompleteBlobName(bucketName));
        blobs.add(blob);
        return blobs;
    }

    private List<Blob> createInvalidBlobList(String bucketName) {
        List<Blob> blobs = new ArrayList<>();
        Blob blob = mock(Blob.class);
        when(blob.getName()).thenReturn(getInvalidTransferCompleteBlobName(bucketName));
        blobs.add(blob);
        return blobs;
    }

    private void setupValidStorageService(List<String> bucketNames) {
        for (var bucketName : bucketNames) {
            List<Blob> blobs = createValidBlobList(bucketName);
            when(storageServiceMock.list(bucketName)).thenReturn(new ListPage(blobs));
        }
    }

    private void setupInvalidStorageService(List<String> bucketNames) {
        for (var bucketName : bucketNames) {
            List<Blob> blobs = createInvalidBlobList(bucketName);
            when(storageServiceMock.list(bucketName)).thenReturn(new ListPage(blobs));
        }
    }

    @Test
    void transferCompleteWithoutResourcesTest() {
        List<String> bucketNames = Arrays.asList("test_bucket");

        TransferProcess transferProcess = createTransferProcess(bucketNames.get(0));
        setupValidStorageService(bucketNames);
        assertThat(statusChecker.isComplete(transferProcess, null)).isTrue();
    }

    @Test
    void transferCompleteWithResourcesTest() {
        List<String> bucketNames = Arrays.asList("test_bucket");

        TransferProcess transferProcess = createTransferProcess(bucketNames.get(0));
        setupValidStorageService(bucketNames);
        List<ProvisionedResource> resources = createResourceList(bucketNames);

        assertThat(statusChecker.isComplete(transferProcess, resources)).isTrue();
    }

    @Test
    void transferNotCompleteWithoutResourcesTest() {
        List<String> bucketNames = Arrays.asList("test_bucket");

        TransferProcess transferProcess = createTransferProcess(bucketNames.get(0));
        setupInvalidStorageService(bucketNames);
        assertThat(statusChecker.isComplete(transferProcess, null)).isFalse();
    }

    @Test
    void transferNotCompleteWithResourcesTest() {
        List<String> bucketNames = Arrays.asList("test_bucket");

        TransferProcess transferProcess = createTransferProcess(bucketNames.get(0));
        setupInvalidStorageService(bucketNames);
        List<ProvisionedResource> resources = createResourceList(bucketNames);

        assertThat(statusChecker.isComplete(transferProcess, resources)).isFalse();
    }
}
