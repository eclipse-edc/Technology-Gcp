/*
 *  Copyright (c) 2022 T-Systems International GmbH
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       T-Systems International GmbH
 *
 */

package org.eclipse.edc.connector.dataplane.gcp.storage;

import com.google.cloud.ReadChannel;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;

import java.io.InputStream;
import java.nio.channels.Channels;
import java.util.Objects;
import java.util.stream.Stream;

import static org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult.success;

public class GcsDataSource implements DataSource {
    private Storage storageClient;
    private String bucketName;
    private String blobName;
    private Monitor monitor;
    private GoogleStoragePart part;

    @Override
    public StreamResult<Stream<Part>> openPartStream() {
        try {
            part = new GoogleStoragePart(storageClient, bucketName, blobName);
            return success(Stream.of(part));
        } catch (Exception e) {
            monitor.severe(String.format("Error accessing bucket %s or blob %s in project %s", bucketName, blobName, storageClient.getOptions().getProjectId()), e);
            throw new EdcException(e);
        }
    }

    @Override
    public void close() throws Exception {
        part.close();
        part = null;
    }

    public static class Builder {
        private final GcsDataSource source;

        private Builder() {
            source = new GcsDataSource();
        }

        public static Builder newInstance() {
            return new Builder();
        }

        public Builder bucketName(String bucketName) {
            source.bucketName = bucketName;
            return this;
        }

        public Builder blobName(String blobName) {
            source.blobName = blobName;
            return this;
        }

        public Builder monitor(Monitor monitor) {
            source.monitor = monitor;
            return this;
        }

        public Builder storageClient(Storage storageClient) {
            source.storageClient = storageClient;
            return this;
        }

        public GcsDataSource build() {
            Objects.requireNonNull(source.monitor, "monitor");
            Objects.requireNonNull(source.storageClient, "storageClient");
            Objects.requireNonNull(source.blobName, "blobName");
            Objects.requireNonNull(source.bucketName, "bucketName");
            return source;
        }
    }

    private static class GoogleStoragePart implements Part {
        private final Storage storageClient;
        private final String bucketName;
        private final String blobName;
        private ReadChannel readChannel;

        private GoogleStoragePart(Storage storageClient, String bucketName, String blobName) {
            this.storageClient = storageClient;
            this.bucketName = bucketName;
            this.blobName = blobName;
        }

        @Override
        public String name() {
            return blobName;
        }

        @Override
        public InputStream openStream() {
            var blobId = BlobId.of(bucketName, blobName);
            var blob = storageClient.get(blobId);
            if (blob != null && blob.exists()) {
                readChannel = storageClient.reader(blobId);
                return Channels.newInputStream(readChannel);
            } else {
                throw new EdcException(String.format("Error accessing bucket %s or blob %s in project %s", bucketName, blobName, storageClient.getOptions().getProjectId()));
            }
        }

        public void close() {
            readChannel.close();
            readChannel = null;
        }
    }
}
