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
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

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
            monitor.info("BigQuery extension for project '" + project + "' using provided credentials");
            return;
        }

        var sourceCredentials = GoogleCredentials.getApplicationDefault()
                .createScoped(IamScopes.CLOUD_PLATFORM);
        sourceCredentials.refreshIfExpired();

        if (serviceAccountName == null) {
            monitor.info("BigQuery extension for project '" + project + "' using ADC, NOT RECOMMENDED");
            credentials = sourceCredentials;
            return;
        }

        monitor.info("BigQuery extension for project '" + project + "' using service account '" + serviceAccountName + "'");
        credentials = ImpersonatedCredentials.create(
                sourceCredentials,
                serviceAccountName,
                null,
                Arrays.asList("https://www.googleapis.com/auth/bigquery"),
                300);
    }

    private void checkStreamWriter()
        throws DescriptorValidationException, IOException, InterruptedException {
        monitor.info("BigQuery creating stream writer in pending mode for table " + target.getTableName().toString());
        if (streamWriter == null) {
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
            monitor.info("BigQuery stream writer closed, recreating it for stream " + streamWriter.getStreamName());
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
            monitor.warning("Cannot refresh BigQuery credentials", ioException);
        }
    }

    public Stream<DataSource.Part> runSourceQuery(String query, DataAddress sinkAddress) throws InterruptedException {
        refreshBigQueryCredentials();

        var queryConfigBuilder = QueryJobConfiguration.newBuilder(query);
        var customerName = sinkAddress.getStringProperty(CUSTOMER_NAME);
        if (customerName != null) {
            queryConfigBuilder.setLabels(ImmutableMap.of("customer", customerName));
        }

        // Use standard SQL syntax for queries.
        // See: https://cloud.google.com/bigquery/sql-reference/
        var queryConfig = queryConfigBuilder
                .setUseLegacySql(false)
                .build();

        var jobId = JobId.of(UUID.randomUUID().toString());
        var queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

        queryJob = queryJob.waitFor();

        if (queryJob == null) {
            throw new GcpException("Job no longer exists");
        } else if (queryJob.getStatus().getError() != null) {
            // Another way is invoking queryJob.getStatus().getExecutionErrors()
            // to get all errors.
            throw new GcpException(queryJob.getStatus().getError().toString());
        }

        var schema = queryJob.getQueryResults().getSchema();
        var partArray = new JSONArray();
        var parts = new ArrayList<DataSource.Part>();
        for (FieldValueList row : queryJob.getQueryResults().iterateAll()) {
            partArray.put(buildRecord(schema.getFields(), row));
        }
        parts.add(new BigQueryPart("allRows", new ByteArrayInputStream(
                partArray.toString().getBytes())));

        return parts.stream();
    }

    private void append(JSONArray data)
        throws DescriptorValidationException, IOException, InterruptedException {
        checkStreamWriter();
        ApiFuture<AppendRowsResponse> future = streamWriter.append(data);
        ApiFutures.addCallback(
                future, new AppendCompleteCallback(), MoreExecutors.directExecutor());
    }

    public void runSinkQuery(List<DataSource.Part> parts) {
        if (parts.isEmpty()) {
            return;
        }

        var errorWhileAppending = false;
        for (var part : parts) {
            try (var inputStream = part.openStream()) {
                var text = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);
                // TODO re-try the append operation for a maximum, specified number of times.
                append(new JSONArray(text));
            } catch (IOException ioException) {
                errorWhileAppending = true;
                monitor.severe("BigQuery error while appending", ioException);
                break;
            } catch (InterruptedException interruptedException) {
                errorWhileAppending = true;
                monitor.severe("BigQuery error while appending", interruptedException);
                break;
            } catch (DescriptorValidationException descriptorValidationException) {
                errorWhileAppending = true;
                monitor.severe("BigQuery error while appending", descriptorValidationException);
                break;
            }
        }

        inflightRequestCount.arriveAndAwaitAdvance();
        streamWriter.close();

        monitor.info("BigQuery requests arrived, stream " + streamWriter.getStreamName() + " for table " + target.getTableName().toString() + " closed, now finalizing..");
        var finalizeResponse =
                writeClient.finalizeWriteStream(streamWriter.getStreamName());
        monitor.info("BigQuery rows written: " + finalizeResponse.getRowCount());

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
                    monitor.severe("BigQuery error while committing the streams " + err.getErrorMessage());
                }
                if (!errorLogged) {
                    monitor.severe("BigQuery error while committing the streams");
                }
            } else {
                monitor.info("BigQuery records committed successfully, write client shutting down...");
            }
        }
        writeClient.shutdownNow();
        var waitIterationCount = 0;
        var terminated = false;
        do {
            try {
                terminated = writeClient.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException interruptedException) {
                monitor.info("BigQuery write client shut down (interrupted)");
            }
            waitIterationCount++;
        } while (!terminated && waitIterationCount < 3);

        if (terminated) {
            monitor.info("BigQuery write client shut down");
        } else {
            monitor.warning("BigQuery write client NOT shut down after timeout");
        }
    }

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

        public void onSuccess(AppendRowsResponse response) {
            monitor.info("Json writer append success");
            inflightRequestCount.arriveAndDeregister();
        }

        public void onFailure(Throwable throwable) {
            monitor.severe("Json writer failed");
            inflightRequestCount.arriveAndDeregister();
            // TODO retry the append operation for a maximum, specified number of times.
        }
    }
}
