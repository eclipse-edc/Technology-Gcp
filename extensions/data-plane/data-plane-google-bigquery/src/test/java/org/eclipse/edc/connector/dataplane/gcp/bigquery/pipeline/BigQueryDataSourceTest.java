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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.QueryResultsOption;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValue.Attribute;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobException;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;
import org.eclipse.edc.connector.dataplane.gcp.bigquery.params.BigQueryRequestParams;
import org.eclipse.edc.gcp.bigquery.BigQueryConfiguration;
import org.eclipse.edc.gcp.bigquery.BigQueryPart;
import org.eclipse.edc.gcp.bigquery.BigQueryTarget;
import org.eclipse.edc.gcp.bigquery.service.BigQueryServiceSchema;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Fail.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BigQueryDataSourceTest {
    private Monitor monitor = mock();
    private GcpConfiguration gcpConfiguration = new GcpConfiguration(TEST_PROJECT, TEST_SOURCE_SERVICE_ACCOUNT_NAME, null, null);
    private BigQueryConfiguration configuration = new BigQueryConfiguration(gcpConfiguration, "testEndpoint", null, 0);
    private BigQuery bigQuery = mock();
    private Table table = mock();
    private TypeManager typeManager = mock();
    private GoogleCredentials credentials = mock();
    private static final String REQUEST_ID = "request-id";
    private static final String TEST_PROJECT = "test-project";
    private static final String TEST_DATASET = "test-dataset";
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_CUSTOMER_NAME = "customer-name";
    private static final String TEST_QUERY_NO_PARAMS = "select * from " + TEST_TABLE;
    private static final String TEST_SOURCE_SERVICE_ACCOUNT_NAME = "sinkAccount";
    private static final String TEST_PARAM_NAME = "argument";

    private static final List<Field> TEST_FIELDS = Arrays.asList(
            Field.of("textfield", LegacySQLTypeName.STRING),
            Field.of("integerfield", LegacySQLTypeName.INTEGER),
            Field.of("booleanfield", LegacySQLTypeName.BOOLEAN)
    );

    private static final List<FieldValue> TEST_ROW_1 = Arrays.asList(
            FieldValue.of(Attribute.PRIMITIVE, "row1 textvalue"),
            FieldValue.of(Attribute.PRIMITIVE, "1"),
            FieldValue.of(Attribute.PRIMITIVE, "false")
    );

    private static final List<FieldValue> TEST_ROW_2 = Arrays.asList(
            FieldValue.of(Attribute.PRIMITIVE, "row2 textvalue"),
            FieldValue.of(Attribute.PRIMITIVE, "-4"),
            FieldValue.of(Attribute.PRIMITIVE, "true")
    );

    private final BigQueryTarget target = new BigQueryTarget(TEST_PROJECT, TEST_DATASET, TEST_TABLE);
    private final ExecutorService executorService = Executors.newFixedThreadPool(2);
    private final String testTokenValue = "test-access-token";
    private final long testTokenExpirationTime = 433454334;

    private final AccessToken credentialAccessToken = AccessToken.newBuilder()
            .setTokenValue(testTokenValue)
            .setExpirationTime(new Date(testTokenExpirationTime))
            .build();

    private Random random;

    @BeforeEach
    void setup() {
        reset(monitor);
        reset(bigQuery);
        reset(table);
        reset(typeManager);

        when(monitor.withPrefix(any(String.class))).thenReturn(monitor);

        when(credentials.createScoped(any(String[].class))).thenReturn(credentials);
        when(credentials.getAccessToken()).thenReturn(credentialAccessToken);

        // New seed for each test.
        random = new Random();
    }

    @Test
    void testOpenPartsWithBigQuerySink() throws InterruptedException {
        var schema = Schema.of(FieldList.of(TEST_FIELDS));

        var tableResult = mock(TableResult.class);
        when(tableResult.getSchema()).thenReturn(schema);
        when(tableResult.getValues()).thenReturn(Arrays.asList(
                FieldValueList.of(TEST_ROW_1),
                FieldValueList.of(TEST_ROW_2)));

        var jobStatus = mock(JobStatus.class);
        when(jobStatus.getError()).thenReturn(null);

        var queryJob = mock(Job.class);
        when(queryJob.getStatus()).thenReturn(jobStatus);

        when(bigQuery.create(any(JobInfo.class))).thenReturn(queryJob);
        when(queryJob.getQueryResults(QueryResultsOption.pageSize(4))).thenReturn(tableResult);

        var options = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId(TEST_PROJECT).build();
        when(bigQuery.getOptions()).thenReturn(options);

        var params = BigQueryRequestParams.Builder.newInstance()
                .project(TEST_PROJECT)
                .dataset(TEST_DATASET)
                .table(TEST_TABLE)
                .query(TEST_QUERY_NO_PARAMS)
                .sinkAddress(getDestinationDataAddress("BigQueryData"))
                .build();

        var dataSource = BigQueryDataSource.Builder.newInstance()
                .monitor(monitor)
                .requestId(REQUEST_ID)
                .params(params)
                .executorService(executorService)
                .credentials(credentials)
                .target(target)
                .configuration(configuration)
                .bigQuery(bigQuery)
                .build();

        var partStream = dataSource.openPartStream();
        assertThat(partStream.succeeded()).isTrue();
        var receivedParts = partStream.getContent();

        verify(bigQuery).create(any(JobInfo.class));

        var receivedList = receivedParts.toList();
        var rows = Arrays.asList(TEST_ROW_1, TEST_ROW_2);
        var expectedParts = Arrays.asList(getPart(TEST_FIELDS, rows));

        assertThat(receivedList.size()).isEqualTo(expectedParts.size());
        for (var i = 0; i < receivedList.size(); i++) {
            if (receivedList.get(i) instanceof BigQueryPart bigQueryPart) {
                assertThat(bigQueryPart.getException()).isNull();
            }
            assertThat(receivedList.get(i)).usingRecursiveComparison().ignoringFields("inputStream").isEqualTo(expectedParts.get(i));
        }
    }

    @Test
    void testOpenPartsWithStandardSink() throws InterruptedException {
        var schema = Schema.of(FieldList.of(TEST_FIELDS));

        var tableResult = mock(TableResult.class);
        when(tableResult.getSchema()).thenReturn(schema);
        when(tableResult.getValues()).thenReturn(Arrays.asList(
                FieldValueList.of(TEST_ROW_1),
                FieldValueList.of(TEST_ROW_2)));

        var jobStatus = mock(JobStatus.class);
        when(jobStatus.getError()).thenReturn(null);

        var queryJob = mock(Job.class);
        when(queryJob.getStatus()).thenReturn(jobStatus);

        when(bigQuery.create(any(JobInfo.class))).thenReturn(queryJob);
        when(queryJob.getQueryResults(QueryResultsOption.pageSize(4))).thenReturn(tableResult);

        var options = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId(TEST_PROJECT).build();
        when(bigQuery.getOptions()).thenReturn(options);

        var params = BigQueryRequestParams.Builder.newInstance()
                .project(TEST_PROJECT)
                .dataset(TEST_DATASET)
                .table(TEST_TABLE)
                .query(TEST_QUERY_NO_PARAMS)
                .sinkAddress(getDestinationDataAddress("StandardSink"))
                .build();

        var dataSource = BigQueryDataSource.Builder.newInstance()
                .monitor(monitor)
                .requestId(REQUEST_ID)
                .params(params)
                .executorService(executorService)
                .credentials(credentials)
                .target(target)
                .configuration(configuration)
                .bigQuery(bigQuery)
                .build();

        var partStream = dataSource.openPartStream();
        assertThat(partStream.succeeded()).isTrue();
        var receivedParts = partStream.getContent();

        verify(bigQuery).create(any(JobInfo.class));

        var receivedList = receivedParts.toList();

        var rows = Arrays.asList(TEST_ROW_1, TEST_ROW_2);
        var expectedParts = Arrays.asList(getPart(TEST_FIELDS, rows));

        assertThat(receivedList.size()).isEqualTo(expectedParts.size());
        for (var i = 0; i < receivedList.size(); i++) {
            if (receivedList.get(i) instanceof BigQueryPart bigQueryPart) {
                assertThat(bigQueryPart.getException()).isNull();
            }
            assertThat(receivedList.get(i)).usingRecursiveComparison().ignoringFields("inputStream").isEqualTo(expectedParts.get(i));
        }
    }

    @Test
    void testOpenPartsThrowsException() throws InterruptedException {
        var schema = Schema.of(FieldList.of(TEST_FIELDS));

        var tableResult = mock(TableResult.class);
        when(tableResult.getSchema()).thenReturn(schema);
        when(tableResult.getValues()).thenReturn(Arrays.asList(
                FieldValueList.of(TEST_ROW_1),
                FieldValueList.of(TEST_ROW_2)));

        var jobStatus = mock(JobStatus.class);
        when(jobStatus.getError()).thenReturn(null);

        var queryJob = mock(Job.class);
        when(queryJob.getStatus()).thenReturn(jobStatus);

        when(bigQuery.create(any(JobInfo.class))).thenReturn(queryJob);
        var jobException = mock(JobException.class);
        when(queryJob.getQueryResults(QueryResultsOption.pageSize(4))).thenThrow(jobException);

        var options = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId(TEST_PROJECT).build();
        when(bigQuery.getOptions()).thenReturn(options);

        var params = BigQueryRequestParams.Builder.newInstance()
                .project(TEST_PROJECT)
                .dataset(TEST_DATASET)
                .table(TEST_TABLE)
                .query(TEST_QUERY_NO_PARAMS)
                .sinkAddress(getDestinationDataAddress("BigQueryData"))
                .build();

        var dataSource = BigQueryDataSource.Builder.newInstance()
                .monitor(monitor)
                .requestId(REQUEST_ID)
                .params(params)
                .executorService(executorService)
                .credentials(credentials)
                .target(target)
                .configuration(configuration)
                .bigQuery(bigQuery)
                .build();

        var partStream = dataSource.openPartStream();
        assertThat(partStream.succeeded()).isTrue();
        var receivedParts = partStream.getContent();

        verify(bigQuery).create(any(JobInfo.class));

        var receivedList = receivedParts.toList();
        for (var i = 0; i < receivedList.size(); i++) {
            if (receivedList.get(i) instanceof BigQueryPart bigQueryPart) {
                try {
                    bigQueryPart.openStream().read();
                    verify(queryJob).getQueryResults(QueryResultsOption.pageSize(4));
                    assertThat(bigQueryPart.getException()).isNotNull();
                } catch (IOException ioException) {
                    fail("Error reading input stream", ioException);
                }
            }
        }
    }

    private JSONObject buildRecord(List<Field> fields, List<FieldValue> values) {
        var record = new JSONObject();
        for (var i = 0; i < fields.size(); i++) {
            record.put(fields.get(i).getName(), values.get(i).getValue());
        }

        return record;
    }

    private BigQueryPart getPart(List<Field> fieldList, List<List<FieldValue>> fieldValueLists) {
        var array = new JSONArray();
        for (var row : fieldValueLists) {
            array.put(buildRecord(fieldList, row));
        }
        return new BigQueryPart("allRows", new ByteArrayInputStream(
            array.toString().getBytes()));
    }

    private static DataAddress getDestinationDataAddress(String type) {
        return DataAddress.Builder.newInstance()
            .type(type)
            .property(BigQueryServiceSchema.PROJECT, TEST_PROJECT)
            .property(BigQueryServiceSchema.DATASET, TEST_DATASET)
            .property(BigQueryServiceSchema.TABLE, TEST_TABLE)
            .property(BigQueryServiceSchema.CUSTOMER_NAME, TEST_CUSTOMER_NAME)
            .property(BigQueryServiceSchema.SERVICE_ACCOUNT_NAME, TEST_SOURCE_SERVICE_ACCOUNT_NAME)
            .build();
    }
}
