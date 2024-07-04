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
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamFailure;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.connector.dataplane.util.sink.ParallelSink;
import org.eclipse.edc.gcp.bigquery.BigQueryConfiguration;
import org.eclipse.edc.gcp.bigquery.BigQueryPart;
import org.eclipse.edc.gcp.bigquery.BigQueryTarget;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.json.JSONArray;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;

import static org.eclipse.edc.connector.dataplane.spi.pipeline.StreamFailure.Reason.GENERAL_ERROR;
import static org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult.failure;
import static org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult.success;

/**
 * Writes JSON data in a streaming fashion using the BigQuery Storage API (RPC).
 */
public class BigQueryDataSink extends ParallelSink {
    /**
     * Write finalization time-out in seconds.
     */
    private static final int BIG_QUERY_WRITE_TIME_OUT_SEC = 2;
    /**
     * Max number of iterations waiting for the write finalization.
     */
    private static final int BIG_QUERY_WRITE_MAX_WAIT_ITERATION = 3;
    private BigQueryConfiguration configuration;
    private BigQueryTarget target;
    private GoogleCredentials credentials;
    private final Phaser inflightRequestCount = new Phaser(1);
    private BigQueryWriteClient writeClient;
    private JsonStreamWriter streamWriter;
    private ObjectMapper objectMapper;

    private BigQueryDataSink() {
    }

    void testAppendSignal() {
        inflightRequestCount.arriveAndDeregister();
    }

    @Override
    protected StreamResult<Object> transferParts(List<DataSource.Part> parts) {
        if (parts.isEmpty()) {
            return StreamResult.success();
        }

        try {
            openStreamWriter();
        } catch (Exception exception) {
            return failure(new StreamFailure(List.of("Error :" + exception), GENERAL_ERROR));
        }

        Exception appendException = null;
        var errorWhileAppending = false;

        for (var part : parts) {
            try (var inputStream = part.openStream()) {
                var jsonParser = objectMapper.createParser(inputStream);
                var token = jsonParser.nextToken();
                while (token == JsonToken.START_ARRAY) {
                    var element = objectMapper.readTree(jsonParser);
                    var jsonString = element.toString();
                    var page = new JSONArray(jsonString);
                    append(page);
                    token = jsonParser.nextToken();
                }

                if (token != null && token != JsonToken.START_ARRAY) {
                    errorWhileAppending = true;
                    appendException = new IllegalStateException("JSON array expected");
                    monitor.severe("Error while appending: JSON array expected");
                    break;
                }
            } catch (Exception exception) {
                errorWhileAppending = true;
                appendException = exception;
                monitor.severe("Error while appending: ", appendException);
                break;
            }

            if (part instanceof BigQueryPart bigQueryPart) {
                if (bigQueryPart.getException() != null) {
                    errorWhileAppending = true;
                    appendException = bigQueryPart.getException();
                    monitor.severe("Error from source: ", appendException);
                    break;
                }
            }
        }

        inflightRequestCount.arriveAndAwaitAdvance();
        closeStreamWriter();

        monitor.debug("Requests arrived, stream " +
                streamWriter.getStreamName() + " for table " + target.getTableName().toString() +
                " closed, now finalizing..");

        cleanupResources(errorWhileAppending);

        if (errorWhileAppending) {
            if (appendException != null) {
                return failure(new StreamFailure(List.of("Error :" + appendException), GENERAL_ERROR));
            }
            return failure(new StreamFailure(List.of("Error"), GENERAL_ERROR));
        }

        return success();
    }

    private void openStreamWriter() throws DescriptorValidationException, IOException, InterruptedException {
        if (streamWriter != null) {
            return;
        }

        monitor.debug("Opening stream writer in pending mode for table " + target.getTableName().toString());
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

    private void closeStreamWriter() {
        if (streamWriter != null && !streamWriter.isClosed()) {
            streamWriter.close();
        }
    }

    private void cleanupResources(boolean errorWhileAppending) {
        var finalizeResponse =
                writeClient.finalizeWriteStream(streamWriter.getStreamName());
        monitor.debug("Rows written: " + finalizeResponse.getRowCount());

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
                terminated = writeClient.awaitTermination(BIG_QUERY_WRITE_TIME_OUT_SEC, TimeUnit.SECONDS);
            } catch (InterruptedException interruptedException) {
                monitor.warning("Write client shut down (interrupted)");
            }
            waitIterationCount++;
        } while (!terminated && waitIterationCount < BIG_QUERY_WRITE_MAX_WAIT_ITERATION);

        if (terminated) {
            monitor.info("Write client shut down");
        } else {
            monitor.warning("Write client NOT shut down after timeout");
        }
    }

    private void initService() throws IOException {
        if (writeClient != null) {
            return;
        }


    }

    private void append(JSONArray page) throws DescriptorValidationException, IOException, InterruptedException {
        // TODO re-try the append operation for a maximum, specified number of times.
        ApiFuture<AppendRowsResponse> future = streamWriter.append(page);
        ApiFutures.addCallback(
                future, new AppendCompleteCallback(), MoreExecutors.directExecutor());
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
                monitor.severe("Error while committing the streams " + err.getErrorMessage());
            }
            if (!errorLogged) {
                monitor.severe("Error while committing the streams");
            }
        } else {
            monitor.debug("Records committed successfully, write client shutting down...");
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
            monitor.severe("JSON writer failed", throwable);
            inflightRequestCount.arriveAndDeregister();
            // TODO retry the append operation for a maximum, specified number of times.
        }
    }

    public static class Builder extends ParallelSink.Builder<Builder, BigQueryDataSink> {
        private Builder() {
            super(new BigQueryDataSink());
        }

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

        @Override
        public Builder monitor(Monitor monitor) {
            sink.monitor = monitor.withPrefix("BigQuery Sink");
            return this;
        }

        public Builder objectMapper(ObjectMapper objectMapper) {
            sink.objectMapper = objectMapper;
            return this;
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

        @Override
        protected void validate() {
            Objects.requireNonNull(sink.configuration, "configuration");
            Objects.requireNonNull(sink.target, "target");
            Objects.requireNonNull(sink.writeClient, "writeClient");
        }

        Builder writeClient(BigQueryWriteClient writeClient) {
            sink.writeClient = writeClient;
            return this;
        }

        Builder streamWriter(JsonStreamWriter streamWriter) {
            sink.streamWriter = streamWriter;
            return this;
        }
    }
}
