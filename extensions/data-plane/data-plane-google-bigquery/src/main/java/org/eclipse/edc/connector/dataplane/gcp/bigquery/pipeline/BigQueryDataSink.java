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

package org.eclipse.edc.connector.dataplane.gcp.bigquery.pipeline;

import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import com.google.protobuf.Descriptors.DescriptorValidationException;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamFailure;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.connector.dataplane.util.sink.ParallelSink;
import org.eclipse.edc.gcp.bigquery.BigQueryConfiguration;
import org.eclipse.edc.gcp.bigquery.BigQueryPart;
import org.eclipse.edc.gcp.bigquery.BigQueryTarget;
import org.eclipse.edc.gcp.common.GcpException;
import org.json.JSONArray;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;

import static org.eclipse.edc.connector.dataplane.spi.pipeline.StreamFailure.Reason.GENERAL_ERROR;

/**
 * Writes JSON data in a streaming fashion using the BigQuery Storage API (RPC).
 */
public class BigQueryDataSink extends ParallelSink {
    private BigQueryConfiguration configuration;
    private BigQueryTarget target;
    private GoogleCredentials credentials;
    private final Phaser inflightRequestCount = new Phaser(1);
    private BigQueryWriteClient writeClient;
    private JsonStreamWriter streamWriter;


    @Override
    protected StreamResult<Object> transferParts(List<DataSource.Part> parts) {
        if (parts.isEmpty()) {
            return StreamResult.success();
        }

        Exception appendException = null;
        var errorWhileAppending = false;
        for (var part : parts) {
            if (part instanceof BigQueryPart bigQueryPart) {
                if (bigQueryPart.getException() != null) {
                    errorWhileAppending = true;
                    appendException = bigQueryPart.getException();
                    monitor.severe("BigQuery Sink error from source: ", appendException);
                    break;
                }
            }

            try (var inputStream = part.openStream()) {
                var mapper = new ObjectMapper();
                var jsonParser2 = mapper.createParser(inputStream);
                if (jsonParser2.nextToken() != JsonToken.START_ARRAY) {
                    throw new IllegalStateException("BigQuery sink JSON array expected");
                }
                var element = mapper.readTree(jsonParser2);
                var jsonString = element.toString();
                var page = new JSONArray(jsonString);
                append(page);
            } catch (Exception exception) {
                errorWhileAppending = true;
                appendException = exception;
                monitor.severe("BigQuery Sink error while appending: ", appendException);
                break;
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

        if (errorWhileAppending) {
            if (appendException != null) {
                return StreamResult.failure(new StreamFailure(List.of("BigQuery Sink error :" + appendException), GENERAL_ERROR));
            }
            return StreamResult.failure(new StreamFailure(List.of("BigQuery Sink error"), GENERAL_ERROR));
        }

        return StreamResult.success();
    }

    private BigQueryDataSink() {
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

            if (configuration.rpcEndpoint() != null) {
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
            if (configuration.rpcEndpoint() != null) {
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
        var host = configuration.rpcEndpoint();
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

    void testAppendSignal() {
        inflightRequestCount.arriveAndDeregister();
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

    public static class Builder extends ParallelSink.Builder<Builder, BigQueryDataSink> {

        public static Builder newInstance() {
            return new Builder();
        }

        public Builder configuration(BigQueryConfiguration configuration) {
            sink.configuration = configuration;
            return this;
        }

        public Builder bigQueryTarget(BigQueryTarget target) {
            sink.target = target;
            return this;
        }

        public Builder credentials(GoogleCredentials credentials) {
            sink.credentials = credentials;
            return this;
        }

        public Builder writeClient(BigQueryWriteClient writeClient) {
            sink.writeClient = writeClient;
            return this;
        }

        public Builder streamWriter(JsonStreamWriter streamWriter) {
            sink.streamWriter = streamWriter;
            return this;
        }

        private Builder() {
            super(new BigQueryDataSink());
        }

        @Override
        protected void validate() {
            // TODO add check for required items.
        }

        @Override
        public BigQueryDataSink build() {
            var sink = super.build();
            try {
                sink.initService();
                return sink;
            } catch (IOException ioException) {
                throw new GcpException(ioException);
            }
        }
    }
}
