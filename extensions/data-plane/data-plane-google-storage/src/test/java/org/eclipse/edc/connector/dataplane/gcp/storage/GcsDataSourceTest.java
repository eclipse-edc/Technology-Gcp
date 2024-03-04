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
 *       Google LLC - Initial implementation
 *
 */

package org.eclipse.edc.connector.dataplane.gcp.storage;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import org.eclipse.edc.spi.monitor.Monitor;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class GcsDataSourceTest {

    Monitor monitor = mock();
    Storage storageClient = mock();
    String bucketName = "TestBucketName";
    String blobName = "TestBlobName";
    GcsDataSource dataSource = GcsDataSource.Builder.newInstance()
            .storageClient(storageClient)
            .monitor(monitor)
            .bucketName(bucketName)
            .blobName(blobName)
            .build();

    BlobId blobId = BlobId.of(bucketName, blobName);


    @Test
    void openPartStream_failsIfBlobIsNull() {
        when(storageClient.get(blobId))
                .thenReturn(null);

        var partStream = dataSource.openPartStream();

        assertThat(partStream.failed()).isTrue();
    }

    @Test
    void openPartStream_failsIfBlobDoesntExist() {
        var blob = mock(Blob.class);

        when(blob.exists())
                .thenReturn(false);

        when(storageClient.get(blobId))
                .thenReturn(blob);

        var partStream = dataSource.openPartStream();

        assertThat(partStream.failed()).isTrue();
    }

    @Test
    void openPartStream_succeedsIfBlobExists() {
        var blob = mock(Blob.class);

        when(blob.exists())
                .thenReturn(true);

        when(storageClient.get(blobId))
                .thenReturn(blob);

        var partStream = dataSource.openPartStream();

        assertThat(partStream.succeeded()).isTrue();
    }
}
