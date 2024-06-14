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
import org.eclipse.edc.connector.dataplane.gcp.bigquery.params.BigQueryRequestParams;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.gcp.bigquery.BigQueryConfiguration;
import org.eclipse.edc.gcp.bigquery.BigQueryPart;
import org.eclipse.edc.gcp.bigquery.BigQueryTarget;
import org.eclipse.edc.gcp.bigquery.service.BigQueryServiceSchema;
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

import static org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult.error;
import static org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult.success;
import static org.eclipse.edc.spi.constants.CoreConstants.EDC_NAMESPACE;

/**
 * Reads data using the BigQuery API (REST) and transfer it in JSON format.
 */
public class BigQueryDataSource implements DataSource {
    private BigQueryRequestParams params;
    private String requestId;
    private Monitor monitor;
    private BigQueryConfiguration configuration;
    private BigQueryTarget target;
    private ExecutorService executorService;
    private BigQuery bigQuery;
    private GoogleCredentials credentials;

    @Override
    public void close() {
    }

    @Override
    public StreamResult<Stream<Part>> openPartStream() {
        try {
            var query = params.getQuery();
            var sinkAddress = params.getSinkAddress();
            var queryConfigBuilder = QueryJobConfiguration.newBuilder(query);
            var destinationTable = params.getDestinationTable();
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

            return success(Stream.of(new BigQueryPart("allRows", inputStream)));
        } catch (GcpException gcpException) {
            monitor.severe("BigQuery Source error while building the query", gcpException);
            return error("BigQuery Source error while building the query");
        } catch (IOException ioException) {
            monitor.severe("BigQuery Source error while opening input stream", ioException);
            return error("BigQuery Source error while opening input stream");
        }
    }

    private void initService() throws IOException {
        if (bigQuery != null) {
            // Skip service initialization in case the service has been provided by the builder.
            return;
        }

        var bqBuilder = BigQueryOptions.newBuilder().setProjectId(configuration.gcpConfiguration().projectId());

        var host = configuration.restEndpoint();
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
        if (configuration.restEndpoint() != null) {
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

    private void setNamedParameters(QueryJobConfiguration.Builder queryConfigBuilder,
                                    DataAddress sinkAddress) {
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

    void closeSourceStream(PipedOutputStream outputStream) {
        try {
            outputStream.close();
            monitor.debug("BigQuery Source output stream closed");
        } catch (IOException ioException) {
            monitor.severe("BigQuery Source exception closing the output stream", ioException);
        }
    }

    private BigQueryDataSource() {
    }

    public static class Builder {
        private final BigQueryDataSource dataSource;

        public static Builder newInstance() {
            return new Builder();
        }

        private Builder() {
            dataSource = new BigQueryDataSource();
        }

        public Builder requestId(String requestId) {
            dataSource.requestId = requestId;
            return this;
        }

        public Builder monitor(Monitor monitor) {
            dataSource.monitor = monitor;
            return this;
        }

        public Builder params(BigQueryRequestParams params) {
            dataSource.params = params;
            return this;
        }

        public Builder configuration(BigQueryConfiguration configuration) {
            dataSource.configuration = configuration;
            return this;
        }

        public Builder target(BigQueryTarget target) {
            dataSource.target = target;
            return this;
        }

        public Builder executorService(ExecutorService executorService) {
            dataSource.executorService = executorService;
            return this;
        }

        public Builder credentials(GoogleCredentials credentials) {
            dataSource.credentials = credentials;
            return this;
        }

        public Builder bigQuery(BigQuery bigQuery) {
            dataSource.bigQuery = bigQuery;
            return this;
        }

        public BigQueryDataSource build() {
            try {
                dataSource.initService();
            } catch (IOException ioException) {
                throw new GcpException("Cannot create BigQuery service", ioException);
            }
            Objects.requireNonNull(dataSource.requestId, "requestId");
            Objects.requireNonNull(dataSource.monitor, "monitor");
            return dataSource;
        }
    }
}
