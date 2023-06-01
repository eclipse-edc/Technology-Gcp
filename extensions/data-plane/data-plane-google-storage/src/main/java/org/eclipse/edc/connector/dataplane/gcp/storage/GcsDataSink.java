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

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.common.io.ByteStreams;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.connector.dataplane.util.sink.ParallelSink;

import java.io.IOException;
import java.nio.channels.Channels;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class GcsDataSink extends ParallelSink {
    private Storage storageClient;
    private String bucketName;
    private String blobName;

    private GcsDataSink() {}

    /**
     * Writes data into an Google storage.
     */
    @Override
    protected StreamResult<Void> transferParts(List<DataSource.Part> parts) {

        for (DataSource.Part part : parts) {
            try (var input = part.openStream()) {
                var sinkBlobName = Optional.ofNullable(blobName)
                        .orElseGet(part::name);
                var destinationBlobInfo = BlobInfo.newBuilder(BlobId.of(bucketName, sinkBlobName)).build();
                try (var writer = storageClient.writer(destinationBlobInfo)) {
                    ByteStreams.copy(input, Channels.newOutputStream(writer));
                }
            } catch (IOException e) {
                monitor.severe("Cannot open the input part", e);
                monitor.severe(e.toString());
                return StreamResult.error("An error");
            } catch (Exception e) {
                monitor.severe("Error writing data to the bucket", e);
                return StreamResult.error("An error");
            }
        }
        return StreamResult.success();
    }

    public static class Builder extends ParallelSink.Builder<Builder, GcsDataSink> {

        private Builder() {
            super(new GcsDataSink());
        }

        public static Builder newInstance() {
            return new Builder();
        }

        public Builder storageClient(Storage storageClient) {
            sink.storageClient = storageClient;
            return this;
        }

        public Builder blobName(String blobName) {
            sink.blobName = blobName;
            return this;
        }

        public Builder bucketName(String bucketName) {
            sink.bucketName = bucketName;
            return this;
        }

        @Override
        protected void validate() {
            Objects.requireNonNull(sink.bucketName, "bucketName");
        }
    }
}
