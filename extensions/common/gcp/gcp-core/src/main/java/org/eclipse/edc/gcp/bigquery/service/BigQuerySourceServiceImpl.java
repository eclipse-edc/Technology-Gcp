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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.QueryResultsOption;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.QueryParameterValue;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.TableResult;
import com.google.common.collect.ImmutableMap;
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
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import static org.eclipse.edc.spi.constants.CoreConstants.EDC_NAMESPACE;

public class BigQuerySourceServiceImpl implements BigQuerySourceService {
    private final GcpConfiguration gcpConfiguration;
    private final BigQueryTarget target;
    private final Monitor monitor;
    private final ExecutorService executorService;
    private BigQuery bigQuery;
    private GoogleCredentials credentials;

    @Override
    public Stream<DataSource.Part> runSourceQuery(String query, String destinationTable, DataAddress sinkAddress) throws InterruptedException {
        refreshBigQueryCredentials();

        var queryConfigBuilder = QueryJobConfiguration.newBuilder(query);
        var customerName = sinkAddress.getStringProperty(BigQueryServiceSchema.CUSTOMER_NAME);
        if (customerName != null) {
            queryConfigBuilder.setLabels(ImmutableMap.of("customer", customerName));
        }

        setNamedParameters(queryConfigBuilder, sinkAddress);

        // Use standard SQL syntax for queries.
        // See: https://cloud.google.com/bigquery/sql-reference/
        queryConfigBuilder = queryConfigBuilder
                .setUseLegacySql(false)
                .setUseQueryCache(true);
        if (destinationTable != null) {
            queryConfigBuilder = queryConfigBuilder.setDestinationTable(target.getTableId(destinationTable));
        }

        var queryConfig = queryConfigBuilder.build();

        var jobId = JobId.of(UUID.randomUUID().toString());
        var jobInfo = JobInfo.newBuilder(queryConfig).setJobId(jobId).build();
        final var queryJob = bigQuery.create(jobInfo);

        // The output stream object is passed to the thread lambda, do not use try with resources.
        final var outputStream = new PipedOutputStream();
        try {
            // The input stream object is passed to the sink thread, do not use try with resources.
            var inputStream = new PipedInputStream(outputStream);
            final var part = new BigQueryPart("allRows", inputStream);
            executorService.submit(() -> {
                try {
                    // TODO set the page size as optional parameter.
                    var paginatedResults = queryJob.getQueryResults(
                            QueryResultsOption.pageSize(4));
                    outputPage(outputStream, paginatedResults);

                    while (paginatedResults.hasNextPage()) {
                        paginatedResults = paginatedResults.getNextPage();
                        outputPage(outputStream, paginatedResults);
                    }
                    monitor.debug("BigQuery Source all pages fetched");
                } catch (IOException | InterruptedException exception) {
                    part.setException(exception);
                    monitor.severe("BigQuery Source exception while sourcing", exception);
                } finally {
                    closeSourceStream(outputStream);
                }
            });

            return Stream.of(new BigQueryPart("allRows", inputStream));
        } catch (IOException ioException) {
            throw new GcpException(ioException);
        }
    }

    public static class Builder {
        private final BigQuerySourceServiceImpl bqService;

        public static Builder newInstance(GcpConfiguration gcpConfiguration,
                BigQueryTarget target,
                Monitor monitor,
                ExecutorService executorService,
                GoogleCredentials credentials) {
            return new Builder(gcpConfiguration, target, monitor, executorService, credentials);
        }

        private Builder(GcpConfiguration gcpConfiguration,
                BigQueryTarget target,
                Monitor monitor,
                ExecutorService executorService,
                GoogleCredentials credentials) {
            bqService = new BigQuerySourceServiceImpl(gcpConfiguration, target, monitor,
                    executorService, credentials);
        }

        Builder bigQuery(BigQuery bigQuery) {
            bqService.bigQuery = bigQuery;
            return this;
        }

        public BigQuerySourceServiceImpl build() {
            try {
                bqService.initService();
                Objects.requireNonNull(bqService.bigQuery, "bigQuery");
                return bqService;
            } catch (IOException ioException) {
                throw new GcpException(ioException);
            }
        }
    }

    void closeSourceStream(PipedOutputStream outputStream) {
        try {
            outputStream.close();
            monitor.debug("BigQuery Source output stream closed");
        } catch (IOException ioException) {
            monitor.severe("BigQuery Source exception closing the output stream", ioException);
        }
    }

    private BigQuerySourceServiceImpl(GcpConfiguration gcpConfiguration,
                BigQueryTarget target,
                Monitor monitor,
                ExecutorService executorService,
                GoogleCredentials credentials) {
        this.gcpConfiguration = gcpConfiguration;
        this.target = target;
        this.monitor = monitor;
        this.executorService = executorService;
        this.credentials = credentials;
    }

    private void initService() throws IOException {
        if (bigQuery != null) {
            // Skip service initialization in case the service has been provided by the builder.
            return;
        }

        var bqBuilder = BigQueryOptions.newBuilder().setProjectId(gcpConfiguration.projectId());

        var host = System.getProperty("EDC_GCP_BQREST");
        if (host != null) {
            bqBuilder.setHost(host);
            bqBuilder.setLocation(host);
            bqBuilder.setCredentials(NoCredentials.getInstance());
        } else {
            bqBuilder.setCredentials(credentials);
        }

        bigQuery = bqBuilder.build().getService();
    }

    private JSONObject buildRecord(FieldList fields, FieldValueList values) {
        var record = new JSONObject();
        int colCount = fields.size();
        for (int i = 0; i < colCount; i++) {
            var field = fields.get(i);
            var name = field.getName();
            var value = values.get(i).getValue();
            if (field.getType() == LegacySQLTypeName.RECORD) {
                var newFields = field.getSubFields();
                var fieldValueList = (FieldValueList) value;
                var recordValue = fieldValueList.get(0).getRecordValue();
                var recordArray = new JSONArray();
                recordArray.put(buildRecord(newFields, recordValue));
                record.put(name, recordArray);
            } else {
                record.put(name, value);
            }
        }

        return record;
    }

    private void refreshBigQueryCredentials() {
        if (System.getProperty("EDC_GCP_BQREST") != null) {
            // If emulator / system test, skip credential refresh.
            return;
        }

        var credentials = bigQuery.getOptions().getCredentials();
        if (credentials instanceof OAuth2Credentials authCredentials) {
            try {
                // TODO check margin and refresh if too close to expire.
                authCredentials.refreshIfExpired();
            } catch (IOException ioException) {
                monitor.warning("BigQuery Service cannot refresh credentials", ioException);
            }
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
}
