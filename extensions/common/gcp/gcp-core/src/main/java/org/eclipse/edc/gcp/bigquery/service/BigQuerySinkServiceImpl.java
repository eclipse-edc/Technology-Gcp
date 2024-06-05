/*
 *  Copyright (c) 2024 Google LLC
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Google LLC
 *
 */

package org.eclipse.edc.gcp.bigquery.service;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.gson.JsonStreamParser;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.gcp.bigquery.BigQueryPart;
import org.eclipse.edc.gcp.bigquery.BigQueryTarget;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.json.JSONArray;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;

public class BigQuerySinkServiceImpl implements BigQuerySinkService {
    private final GcpConfiguration gcpConfiguration;
    private final BigQueryTarget target;
    private final Monitor monitor;
    private final Phaser inflightRequestCount = new Phaser(1);
    private BigQueryWriteClient writeClient;
    private JsonStreamWriter streamWriter;
    private GoogleCredentials credentials;

    @Override
    public void runSinkQuery(List<DataSource.Part> parts) {
        if (parts.isEmpty()) {
            return;
        }

        var errorWhileAppending = false;
        for (var part : parts) {
            try (var inputStreamReader = new InputStreamReader(part.openStream())) {
                var jsonParser = new JsonStreamParser(inputStreamReader);
                while (jsonParser.hasNext()) {
                    var element = jsonParser.next();
                    var jsonString = element.toString();
                    var page = new JSONArray(jsonString);
                    append(page);
                }
            } catch (IOException | InterruptedException | DescriptorValidationException exception) {
                errorWhileAppending = true;
                monitor.severe("BigQuery Sink error while appending", exception);
                break;
            }

            if (part instanceof BigQueryPart bigQueryPart) {
                if (bigQueryPart.getException() != null) {
                    errorWhileAppending = true;
                    monitor.severe("BigQuery Sink error from source :", bigQueryPart.getException());
                    break;
                }
            }
        }

        inflightRequestCount.arriveAndAwaitAdvance();
        closeSinkStream();

        monitor.debug("BigQuery Sink requests arrived, stream " + streamWriter.getStreamName() + " for table " + target.getTableName().toString() + " closed, now finalizing..");
        var finalizeResponse =
                writeClient.finalizeWriteStream(streamWriter.getStreamName());
        monitor.debug("BigQuery Sink rows written: " + finalizeResponse.getRowCount());

        // Commit the data stream received only if the stream writer succeeded (transaction).
        // TODO support option to enable commit even if the stream writer failed.
        if (!errorWhileAppending) {
            commitData();
        }

        writeClient.shutdownNow();
        var waitIterationCount = 0;
        var terminated = false;
        do {
            try {
                terminated = writeClient.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException interruptedException) {
                monitor.warning("BigQuery Sink write client shut down (interrupted)");
            }
            waitIterationCount++;
        } while (!terminated && waitIterationCount < 3);

        if (terminated) {
            monitor.info("BigQuery Sink write client shut down");
        } else {
            monitor.warning("BigQuery Sink write client NOT shut down after timeout");
        }
    }

    public static class Builder {
        private final BigQuerySinkServiceImpl bqSinkService;

        public static Builder newInstance(GcpConfiguration gcpConfiguration, BigQueryTarget target,
                Monitor monitor) {
            return new Builder(gcpConfiguration, target, monitor);
        }

        private Builder(GcpConfiguration gcpConfiguration, BigQueryTarget target, Monitor monitor) {
            bqSinkService = new BigQuerySinkServiceImpl(gcpConfiguration, target, monitor);
        }

        Builder writeClient(BigQueryWriteClient writeClient) {
            bqSinkService.writeClient = writeClient;
            return this;
        }

        Builder streamWriter(JsonStreamWriter streamWriter) {
            bqSinkService.streamWriter = streamWriter;
            return this;
        }

        public Builder credentials(GoogleCredentials credentials) {
            bqSinkService.credentials = credentials;
            return this;
        }

        public BigQuerySinkServiceImpl build() {
            try {
                bqSinkService.initService();
                Objects.requireNonNull(bqSinkService.writeClient, "writeClient");
                return bqSinkService;
            } catch (IOException ioException) {
                throw new GcpException(ioException);
            }
        }
    }

    void testAppendSignal() {
        inflightRequestCount.arriveAndDeregister();
    }

    private BigQuerySinkServiceImpl(GcpConfiguration gcpConfiguration, BigQueryTarget target, Monitor monitor) {
        this.gcpConfiguration = gcpConfiguration;
        this.target = target;
        this.monitor = monitor;
    }

    private void checkStreamWriter()
            throws DescriptorValidationException, IOException, InterruptedException {
        if (streamWriter == null) {
            monitor.debug("BigQuery Sink creating stream writer in pending mode for table " + target.getTableName().toString());
            var stream = WriteStream.newBuilder()
                    .setType(WriteStream.Type.PENDING)
                    .build();

            var createWriteStreamRequest = CreateWriteStreamRequest.newBuilder()
                    .setParent(target.getTableName().toString())
                    .setWriteStream(stream)
                    .build();

            var writeStream = writeClient.createWriteStream(createWriteStreamRequest);

            var builder = JsonStreamWriter.newBuilder(writeStream.getName(), writeClient);

            if (System.getProperty("EDC_GCP_HOST") != null) {
                builder.setCredentialsProvider(NoCredentialsProvider.create());
            } else {
                builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
            }

            streamWriter = builder.build();
        }

        if (streamWriter.isClosed()) {
            // TODO re-create the writer for a maximum, specified number of times.
            monitor.info("BigQuery Sink stream writer closed, recreating it for stream " + streamWriter.getStreamName());
            var builder = JsonStreamWriter.newBuilder(streamWriter.getStreamName(), writeClient);
            if (System.getProperty("EDC_GCP_HOST") != null) {
                builder.setCredentialsProvider(NoCredentialsProvider.create());
            } else {
                builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
            }
            streamWriter = builder.build();
        }
    }

    private void initService() throws IOException {
        if (writeClient != null) {
            return;
        }

        var settingsBuilder = BigQueryWriteSettings.newBuilder();
        var host = System.getProperty("EDC_GCP_BQRPC");
        if (host != null) {
            var index = host.indexOf("//");
            if (index != -1) {
                host = host.substring(index + 2);
            }
            settingsBuilder.setEndpoint(host);
            settingsBuilder.setTransportChannelProvider(
                    FixedTransportChannelProvider.create(
                        GrpcTransportChannel.create(
                        NettyChannelBuilder.forTarget(host).usePlaintext().build()
                    )
                )
            );

            settingsBuilder.setCredentialsProvider(NoCredentialsProvider.create());
        } else {
            settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
        }
        writeClient = BigQueryWriteClient.create(settingsBuilder.build());
    }

    private void append(JSONArray page)
            throws DescriptorValidationException, IOException, InterruptedException {
        // TODO re-try the append operation for a maximum, specified number of times.
        checkStreamWriter();
        ApiFuture<AppendRowsResponse> future = streamWriter.append(page);
        ApiFutures.addCallback(
                future, new AppendCompleteCallback(), MoreExecutors.directExecutor());
    }

    private boolean closeSinkStream() {
        try {
            checkStreamWriter();
            streamWriter.close();
            return true;
        } catch (IOException | InterruptedException | DescriptorValidationException exception) {
            monitor.severe("BigQuery Sink error while closing the stream writer", exception);
        }

        return false;
    }

    private void commitData() {
        var commitRequest =
                BatchCommitWriteStreamsRequest.newBuilder()
                .setParent(target.getTableName().toString())
                .addWriteStreams(streamWriter.getStreamName())
                .build();
        var commitResponse = writeClient.batchCommitWriteStreams(commitRequest);

        if (!commitResponse.hasCommitTime()) {
            var errorLogged = false;
            for (var err : commitResponse.getStreamErrorsList()) {
                errorLogged = true;
                monitor.severe("BigQuery Sink error while committing the streams " + err.getErrorMessage());
            }
            if (!errorLogged) {
                monitor.severe("BigQuery Sink error while committing the streams");
            }
        } else {
            monitor.debug("BigQuery Sink records committed successfully, write client shutting down...");
        }
    }

    private class AppendCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {
        AppendCompleteCallback() {
            inflightRequestCount.register();
        }

        @Override
        public void onSuccess(AppendRowsResponse response) {
            inflightRequestCount.arriveAndDeregister();
        }

        @Override
        public void onFailure(Throwable throwable) {
            monitor.severe("BigQuery Json writer failed", throwable);
            inflightRequestCount.arriveAndDeregister();
            // TODO retry the append operation for a maximum, specified number of times.
        }
    }
}
