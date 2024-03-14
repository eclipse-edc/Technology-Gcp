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

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.PageImpl;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQuery.QueryResultsOption;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldList;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValue.Attribute;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.JobStatus;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableResult;
import org.eclipse.edc.gcp.bigquery.BigQueryPart;
import org.eclipse.edc.gcp.bigquery.BigQueryTarget;
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
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.eclipse.edc.gcp.bigquery.service.Asserts.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BigQuerySourceServiceImplTest {
    private static final String TEST_PROJECT = "test-project";
    private static final String TEST_DATASET = "test-dataset";
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_QUERY = "select * from " + TEST_TABLE;
    private static final String TEST_CUSTOMER_NAME = "customer_name";
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
    private final ExecutorService executionPool = Executors.newFixedThreadPool(2);
    private Random random;
    private BigQuerySourceServiceImpl bqSourceService;
    private Monitor monitor = mock();
    private GcpConfiguration gcpConfiguration = mock();
    private BigQuery bigQuery = mock();
    private Table table = mock();
    private TypeManager typeManager = mock();

    @BeforeEach
    void setUp() {
        reset(gcpConfiguration);
        reset(monitor);
        reset(bigQuery);
        reset(table);
        reset(typeManager);

        bqSourceService = BigQuerySourceServiceImpl.Builder.newInstance(gcpConfiguration, target, monitor, executionPool)
                .bigQuery(bigQuery)
                .build();

        // New seed for each test.
        random = new Random();
    }

    @Test
    void testRunSourceQueryWithBigQuerySink() throws InterruptedException, IOException {
        testRunSourceQuery("BigQueryData", false);
    }

    @Test
    void testRunSourceQueryWithStandardSink() throws InterruptedException, IOException {
        testRunSourceQuery("StandardSink", false);
    }

    private JSONObject buildRecord(List<Field> fields, List<FieldValue> values) {
        var record = new JSONObject();
        for (var i = 0; i < fields.size(); i++) {
            record.put(fields.get(i).getName(), values.get(i).getValue());
        }

        return record;
    }

    private void testRunSourceQuery(String sinkType, boolean queryException) throws InterruptedException, IOException {
        var sinkAddress = DataAddress.Builder.newInstance()
                .type(sinkType)
                .property("customerName", TEST_CUSTOMER_NAME)
                .build();

        var schema = Schema.of(FieldList.of(TEST_FIELDS));
        var page = new PageImpl<FieldValueList>(
                new PageImpl.NextPageFetcher<FieldValueList>() {
                    @Override
                    public Page<FieldValueList> getNextPage() {
                        return null;
                    }
                },
                null,
                Arrays.asList(
                FieldValueList.of(TEST_ROW_1),
                FieldValueList.of(TEST_ROW_2)
            ));
        var tableResult = new TableResult(schema, 2, page, "queryId");

        var jobStatus = mock(JobStatus.class);
        when(jobStatus.getError()).thenReturn(null);

        var queryJob = mock(Job.class);
        when(queryJob.getStatus()).thenReturn(jobStatus);

        when(bigQuery.create(any(JobInfo.class))).thenReturn(queryJob);
        if (!queryException) {
            when(queryJob.getQueryResults(QueryResultsOption.pageSize(4))).thenReturn(tableResult);
        } else {
            when(queryJob.getQueryResults(QueryResultsOption.pageSize(4))).thenThrow(new IOException());
        }

        var credentials = mock(OAuth2Credentials.class);
        when(credentials.getAccessToken()).thenReturn(null);

        var options = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId(TEST_PROJECT).build();
        when(bigQuery.getOptions()).thenReturn(options);

        var receivedParts = bqSourceService.runSourceQuery(TEST_QUERY, sinkAddress);

        verify(credentials).refreshIfExpired();
        verify(bigQuery).create(any(JobInfo.class));

        var receivedList = receivedParts.toList();
        if (!queryException) {
            var rows = Arrays.asList(TEST_ROW_1, TEST_ROW_2);
            var expectedParts = Arrays.asList(getPart(TEST_FIELDS, rows));

            assertThat(receivedList.size()).isEqualTo(expectedParts.size());
            for (var i = 0; i < receivedList.size(); i++) {
                if (receivedList.get(i) instanceof BigQueryPart bigQueryPart) {
                    assertThat(bigQueryPart.getException()).isNull();
                }
                Asserts.assertThat(receivedList.get(i)).isEqualTo(expectedParts.get(i));
            }
        } else {
            for (var i = 0; i < receivedList.size(); i++) {
                if (receivedList.get(i) instanceof BigQueryPart bigQueryPart) {
                    assertThat(bigQueryPart.getException()).isNotNull();
                }
            }
        }
    }

    private BigQueryPart getPart(List<Field> fieldList, List<List<FieldValue>> fieldValueLists) {
        var array = new JSONArray();
        for (var row : fieldValueLists) {
            array.put(buildRecord(fieldList, row));
        }
        return new BigQueryPart("allRows", new ByteArrayInputStream(
            array.toString().getBytes()));
    }
}
