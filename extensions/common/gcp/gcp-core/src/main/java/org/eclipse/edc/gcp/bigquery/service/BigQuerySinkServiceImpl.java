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
import com.google.api.services.iam.v2.IamScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.gcp.bigquery.BigQueryPart;
import org.eclipse.edc.gcp.bigquery.BigQueryTarget;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;

import static org.eclipse.edc.spi.CoreConstants.EDC_NAMESPACE;

public class BigQuerySinkServiceImpl implements BigQuerySinkService {
    private final GcpConfiguration gcpConfiguration;
    private final BigQueryTarget target;
    private final Monitor monitor;
    private final Phaser inflightRequestCount = new Phaser(1);
    private BigQueryWriteClient writeClient;
    private JsonStreamWriter streamWriter;
    private GoogleCredentials credentials;
    private String serviceAccountName;

    @Override
    public void runSinkQuery(List<DataSource.Part> parts) {
        if (parts.isEmpty()) {
            return;
        }

        CircularBuffer buffer = new CircularBuffer(16384);
        var errorWhileAppending = false;
        for (var part : parts) {
            buffer.reset();
            try (var inputStream = part.openStream()) {
                boolean done = false;
                while (!done) {
                    var available = buffer.getAvailableWriteCount();
                    var dataRead = inputStream.read(buffer.getBuffer(), buffer.getWriteOffset(), available);
                    if (dataRead > 0) {
                        buffer.write(dataRead);
                        append(buffer);
                    } else if (dataRead == -1) {
                        done = true;
                    } else {
                        // dataRead is 0, no space available for writing new content.
                        done = true;
                        errorWhileAppending = true;
                        monitor.severe("BigQuery Sink input buffer full");
                    }
                }

                append(buffer);

                if (buffer.getAvailableReadCount() > 0) {
                    // There are leftover, means some data has not been parsed.
                    errorWhileAppending = true;
                    monitor.severe("BigQuery Sink cannot parse whole input data, remaining " + buffer.getAvailableReadCount());
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

        public static Builder newInstance(GcpConfiguration gcpConfiguration, BigQueryTarget target, Monitor monitor) {
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

        public Builder serviceAccount(String serviceAccountName) {
            bqSinkService.serviceAccountName = serviceAccountName;
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

    private void initCredentials() throws IOException {
        var project = target.project();
        if (project == null) {
            project = gcpConfiguration.getProjectId();
        }

        if (credentials != null) {
            monitor.debug("BigQuery Service for project '" + project + "' using provided credentials");
            return;
        }

        var sourceCredentials = GoogleCredentials.getApplicationDefault()
                .createScoped(IamScopes.CLOUD_PLATFORM);
        sourceCredentials.refreshIfExpired();

        if (serviceAccountName == null) {
            monitor.warning("BigQuery Service for project '" + project + "' using ADC, NOT RECOMMENDED");
            credentials = sourceCredentials;
            return;
        }

        monitor.debug("BigQuery Service for project '" + project + "' using service account '" + serviceAccountName + "'");
        credentials = ImpersonatedCredentials.create(
            sourceCredentials,
            serviceAccountName,
            null,
            Arrays.asList("https://www.googleapis.com/auth/bigquery"),
            3600);
    }

    private void checkStreamWriter()
            throws DescriptorValidationException, IOException, InterruptedException {
        if (streamWriter == null) {
            monitor.debug("BigQuery Sink creating stream writer in pending mode for table " + target.getTableName().toString());
            var stream = WriteStream.newBuilder().setType(WriteStream.Type.PENDING).build();

            var createWriteStreamRequest = CreateWriteStreamRequest.newBuilder()
                    .setParent(target.getTableName().toString())
                    .setWriteStream(stream)
                    .build();
            var writeStream = writeClient.createWriteStream(createWriteStreamRequest);

            var builder = JsonStreamWriter.newBuilder(writeStream.getName(), writeClient);
            if (credentials != null) {
                builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
            }

            streamWriter = builder.build();
        }

        if (streamWriter.isClosed()) {
            // TODO re-create the writer for a maximum, specified number of times.
            monitor.info("BigQuery Sink stream writer closed, recreating it for stream " + streamWriter.getStreamName());
            var builder = JsonStreamWriter.newBuilder(streamWriter.getStreamName(), writeClient);
            if (credentials != null) {
                builder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
            }
            streamWriter = builder.build();
        }
    }

    private void initService() throws IOException {
        if (writeClient != null) {
            return;
        }

        initCredentials();

        var project = target.project();
        if (project == null) {
            project = gcpConfiguration.getProjectId();
        }

        var bqBuilder = BigQueryOptions.newBuilder().setProjectId(project);

        if (credentials != null) {
            bqBuilder.setCredentials(credentials);
        }

        var settingsBuilder = BigQueryWriteSettings.newBuilder();
        if (credentials != null) {
            settingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
        }

        writeClient = BigQueryWriteClient.create(settingsBuilder.build());
    }

    private JSONObject buildRecord(FieldList fields, FieldValueList values) {
        var record = new JSONObject();
        int colCount = fields.size();
        for (int i = 0; i < colCount; i++) {
            record.put(fields.get(i).getName(), values.get(i).getValue());
        }

        return record;
    }

    private void outputPage(OutputStream outputStream, TableResult tableResult) throws IOException {
        var page = new JSONArray();

        tableResult.getValues()
                .forEach(row -> page.put(buildRecord(tableResult.getSchema().getFields(), row)));

        outputStream.write(page.toString().getBytes());
    }

    private void setNamedParameters(QueryJobConfiguration.Builder queryConfigBuilder, DataAddress sinkAddress) {
        var prefix = EDC_NAMESPACE + '@';
        for (var key : sinkAddress.getProperties().keySet()) {
            if (key.startsWith(prefix)) {
                key = key.substring(EDC_NAMESPACE.length());

                var value = sinkAddress.getStringProperty(key);
                key = key.substring(1);
                int separatorIndex = key.indexOf("_");
                if (separatorIndex != -1) {
                    var type = key.substring(0, separatorIndex);
                    key = key.substring(separatorIndex + 1);
                    var sqlValue = QueryParameterValue.of(value, StandardSQLTypeName.valueOf(type));
                    queryConfigBuilder.addNamedParameter(key, sqlValue);
                }
            }
        }
    }

    private void append(CircularBuffer buffer)
            throws DescriptorValidationException, IOException, InterruptedException {
        while (buffer.getAvailableReadCount() > 0) {
            var content = buffer.getString();
            if (!content.startsWith("[")) {
                throw new IOException("BigQuery Sink invalid JSON Array '" + content + "'");
            }

            int endIndex = content.indexOf(']');
            if (endIndex != -1) {
                var pageString = content.substring(0, endIndex + 1);
                var page = new JSONArray(pageString);
                // TODO re-try the append operation for a maximum, specified number of times.
                checkStreamWriter();
                ApiFuture<AppendRowsResponse> future = streamWriter.append(page);
                ApiFutures.addCallback(
                        future, new AppendCompleteCallback(), MoreExecutors.directExecutor());

                buffer.read(pageString.length());
            } else {
                return;
            }
        }
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

    static class CircularBuffer {
        private byte[] buffer;
        private int writeOffset;
        private int readOffset;

        CircularBuffer(int size) {
            buffer = new byte[size];
            reset();
        }

        void reset() {
            writeOffset = 0;
            readOffset = 0;
        }

        byte[] getBuffer() {
            return buffer;
        }

        int getWriteOffset() {
            return writeOffset;
        }

        int getReadOffset() {
            return readOffset;
        }

        int getAvailableWriteCount() {
            if (writeOffset >= readOffset) {
                if (readOffset == 0) {
                    // Avoid filling the whole buffer, otherwise readOffset == writeOffset and read size == 0.
                    return buffer.length - writeOffset - 1;
                } else {
                    return buffer.length - writeOffset;
                }
            }

            // Avoid filling the whole buffer, otherwise readOffset == writeOffset and read size == 0.
            return readOffset - writeOffset - 1;
        }

        int getAvailableReadCount() {
            if (readOffset <= writeOffset) {
                return writeOffset - readOffset;
            }

            return (buffer.length - readOffset) + writeOffset;
        }

        void write(int size) {
            if (size > getAvailableWriteCount()) {
                throw new GcpException("BigQuery buffer cannot write " + size + " bytes, max count is " + getAvailableWriteCount());
            }

            writeOffset += size;
            while (writeOffset >= buffer.length) {
                writeOffset -= buffer.length;
            }
        }

        void read(int size) {
            if (size > getAvailableReadCount()) {
                throw new GcpException("BigQuery buffer cannot read " + size + " bytes, max count is " + getAvailableReadCount());
            }

            readOffset += size;
            while (readOffset >= buffer.length) {
                readOffset -= buffer.length;
            }
        }

        String getString1() {
            if (readOffset <= writeOffset) {
                return new String(buffer, readOffset, writeOffset - readOffset);
            }

            return new String(buffer, readOffset, buffer.length - readOffset);
        }

        String getString2() {
            if (readOffset <= writeOffset) {
                return "";
            }

            return new String(buffer, 0, writeOffset);
        }

        String getString() {
            return getString1() + getString2();
        }
    }
}
