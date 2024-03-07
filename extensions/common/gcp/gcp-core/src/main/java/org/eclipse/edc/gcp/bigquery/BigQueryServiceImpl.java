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

package org.eclipse.edc.gcp.bigquery;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.services.iam.v2.IamScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.QueryResultsOption;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
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
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.json.JSONArray;
import org.json.JSONObject;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.eclipse.edc.spi.CoreConstants.EDC_NAMESPACE;

public class BigQueryServiceImpl implements BigQueryService {
    private final GcpConfiguration gcpConfiguration;
    private final BigQueryTarget target;
    private final Monitor monitor;
    private final Phaser inflightRequestCount = new Phaser(1);
    private BigQuery bigQuery;
    private BigQueryWriteClient writeClient;
    private JsonStreamWriter streamWriter;
    private GoogleCredentials credentials;
    private String serviceAccountName;

    private BigQueryServiceImpl(GcpConfiguration gcpConfiguration, BigQueryTarget target, Monitor monitor) {
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
                300);
    }

    private void checkStreamWriter()
        throws DescriptorValidationException, IOException, InterruptedException {
        if (streamWriter == null) {
            monitor.debug("BigQuery Sink creating stream writer in pending mode for table " + target.getTableName().toString());
            var stream = WriteStream.newBuilder().setType(WriteStream.Type.PENDING).build();

            var createWriteStreamRequest =
                    CreateWriteStreamRequest.newBuilder()
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
        if (bigQuery != null && writeClient != null) {
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

        bigQuery = bqBuilder.build().getService();

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

    private void refreshBigQueryCredentials() {
        var credentials = bigQuery.getOptions().getCredentials();
        if (!(credentials instanceof OAuth2Credentials)) {
            return;
        }

        try {
            // TODO check margin and refresh if too close to expire.
            ((OAuth2Credentials) credentials).refreshIfExpired();
        } catch (IOException ioException) {
            monitor.warning("BigQuery Service cannot refresh credentials", ioException);
        }
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

    @Override
    public Stream<DataSource.Part> runSourceQuery(String query, DataAddress sinkAddress) throws InterruptedException {
        refreshBigQueryCredentials();

        var queryConfigBuilder = QueryJobConfiguration.newBuilder(query);
        var customerName = sinkAddress.getStringProperty(CUSTOMER_NAME);
        if (customerName != null) {
            queryConfigBuilder.setLabels(ImmutableMap.of("customer", customerName));
        }

        setNamedParameters(queryConfigBuilder, sinkAddress);

        // Use standard SQL syntax for queries.
        // See: https://cloud.google.com/bigquery/sql-reference/
        var queryConfig = queryConfigBuilder
                .setUseLegacySql(false)
                .setUseQueryCache(true)
                .build();

        var jobId = JobId.of(UUID.randomUUID().toString());
        var jobInfo = JobInfo.newBuilder(queryConfig).setJobId(jobId).build();
        final var queryJob = bigQuery.create(jobInfo);

        var parts = new ArrayList<DataSource.Part>();
        // The output stream object is passed to the thread lambda, do not use try with resources.
        final var outputStream = new PipedOutputStream();
        try {
            // The input stream object is passed to the sink thread, do not use try with resources.
            var inputStream = new PipedInputStream(outputStream);
            parts.add(new BigQueryPart("allRows", inputStream));

            new Thread(() -> {
                try {
                    // TODO set the page size as optional parameter.
                    var paginatedResults = queryJob.getQueryResults(
                            QueryResultsOption.pageSize(4));
                    outputPage(outputStream, paginatedResults);

                    while (paginatedResults.hasNextPage()) {
                        paginatedResults = paginatedResults.getNextPage();
                        outputPage(outputStream, paginatedResults);
                    }

                } catch (IOException | InterruptedException exception) {
                    monitor.severe("BigQuery Source exception while sourcing", exception);
                } finally {
                    try {
                        outputStream.close();
                        monitor.debug("BigQuery Source output stream closed");
                    } catch (IOException ioException) {
                        monitor.severe("BigQuery Source exception closing the output stream", ioException);
                    }
                }
            }).start();

            return parts.stream();
        } catch (IOException ioException) {
            throw new GcpException(ioException);
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

    private boolean closeStream() {
        try {
            checkStreamWriter();
            streamWriter.close();
            return true;
        } catch (IOException | InterruptedException | DescriptorValidationException exception) {
            monitor.severe("BigQuery Sink error while closing the stream writer", exception);
        }

        return false;
    }

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
                    } else if (dataRead == 0) {
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
        }

        inflightRequestCount.arriveAndAwaitAdvance();
        closeStream();

        monitor.debug("BigQuery Sink requests arrived, stream " + streamWriter.getStreamName() + " for table " + target.getTableName().toString() + " closed, now finalizing..");
        var finalizeResponse =
                writeClient.finalizeWriteStream(streamWriter.getStreamName());
        monitor.debug("BigQuery Sink rows written: " + finalizeResponse.getRowCount());

        // Commit the data stream received only if the stream writer succeeded (transaction).
        // TODO support option to enable commit even if the stream writer failed.
        if (!errorWhileAppending) {
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

    @Override
    public boolean tableExists(BigQueryTarget target) {
        try {
            var table = bigQuery.getTable(target.getTableId());
            if (table != null && table.exists()) {
                return true;
            }

            return false;
        } catch (BigQueryException bigQueryException) {
            monitor.debug(bigQueryException.toString());
            return false;
        }
    }

    public static class Builder {
        private final BigQueryServiceImpl bqService;

        public static Builder newInstance(GcpConfiguration gcpConfiguration, BigQueryTarget target, Monitor monitor) {
            return new Builder(gcpConfiguration, target, monitor);
        }

        private Builder(GcpConfiguration gcpConfiguration, BigQueryTarget target, Monitor monitor) {
            bqService = new BigQueryServiceImpl(gcpConfiguration, target, monitor);
        }

        public Builder credentials(GoogleCredentials credentials) {
            bqService.credentials = credentials;
            return this;
        }

        public Builder serviceAccount(String serviceAccountName) {
            bqService.serviceAccountName = serviceAccountName;
            return this;
        }

        Builder bigQuery(BigQuery bigQuery) {
            bqService.bigQuery = bigQuery;
            return this;
        }

        Builder writeClient(BigQueryWriteClient writeClient) {
            bqService.writeClient = writeClient;
            return this;
        }

        Builder streamWriter(JsonStreamWriter streamWriter) {
            bqService.streamWriter = streamWriter;
            return this;
        }

        public BigQueryServiceImpl build() {
            try {
                bqService.initService();
                Objects.requireNonNull(bqService.bigQuery, "bigQuery");
                return bqService;
            } catch (IOException ioException) {
                throw new GcpException(ioException);
            }
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
                return new String("");
            }

            return new String(buffer, 0, writeOffset);
        }

        String getString() {
            return getString1() + getString2();
        }
    }
}
