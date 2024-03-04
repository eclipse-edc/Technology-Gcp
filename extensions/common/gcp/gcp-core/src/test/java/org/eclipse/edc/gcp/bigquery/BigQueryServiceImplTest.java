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
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.bigquery.BigQuery;
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
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource.Part;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.eclipse.edc.gcp.bigquery.Asserts.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BigQueryServiceImplTest {
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
    private BigQueryServiceImpl bigQueryService;
    private Monitor monitor = mock();
    private GcpConfiguration gcpConfiguration = mock();
    private BigQuery bigQuery = mock();
    private BigQueryWriteClient writeClient = mock();
    private Table table = mock();
    private TypeManager typeManager = mock();
    private JsonStreamWriter streamWriter = mock();

    @BeforeEach
    void setUp() {
        reset(gcpConfiguration);
        reset(monitor);
        reset(bigQuery);
        reset(writeClient);
        reset(table);
        reset(typeManager);
        reset(streamWriter);

        bigQueryService = BigQueryServiceImpl.Builder.newInstance(gcpConfiguration, target, monitor)
            .bigQuery(bigQuery)
            .writeClient(writeClient)
            .streamWriter(streamWriter)
            .build();
    }

    @Test
    void testTableExistsTrue() {
        when(table.exists()).thenReturn(true);
        when(bigQuery.getTable(target.getTableId())).thenReturn(table);

        assertThat(bigQueryService.tableExists(target)).isTrue();
    }

    @Test
    void testTableExistsFalse() {
        when(table.exists()).thenReturn(false);
        when(bigQuery.getTable(target.getTableId())).thenReturn(table);

        assertThat(bigQueryService.tableExists(target)).isFalse();
    }

    private void testRunSinkQuery(boolean succeeds) throws DescriptorValidationException, IOException, InterruptedException, ExecutionException, TimeoutException {
        var parts = new ArrayList<Part>();
        parts.add(getPart(TEST_FIELDS, Arrays.asList(TEST_ROW_1, TEST_ROW_2)));
        var partsCount = parts.stream().mapToLong(o -> o.size()).sum();

        if (succeeds) {
            ApiFuture<AppendRowsResponse> future = mock();
            AppendRowsResponse response = mock();
            when(future.isDone()).thenReturn(true);
            when(future.cancel(anyBoolean())).thenReturn(false);
            when(future.isCancelled()).thenReturn(false);
            doNothing().when(future).addListener(any(Runnable.class), any(Executor.class));
            when(future.get()).thenReturn(response);
            when(future.get(anyLong(), any())).thenReturn(response);
            when(streamWriter.append(any())).thenReturn(future);
        } else {
            when(streamWriter.append(any())).thenThrow(new IOException());
        }
        when(streamWriter.isClosed()).thenReturn(false);
        when(streamWriter.getStreamName()).thenReturn("name");
        when(writeClient.finalizeWriteStream(streamWriter.getStreamName())).thenReturn(
                FinalizeWriteStreamResponse.newBuilder().setRowCount(partsCount).build());

        var commitResponse = mock(BatchCommitWriteStreamsResponse.class);
        when(commitResponse.hasCommitTime()).thenReturn(true);
        when(commitResponse.getStreamErrorsList()).thenReturn(Arrays.asList());
        when(writeClient.batchCommitWriteStreams(any(BatchCommitWriteStreamsRequest.class))).thenReturn(commitResponse);

        var executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.schedule(() -> bigQueryService.testAppendSignal(), 2, TimeUnit.SECONDS);

        bigQueryService.runSinkQuery(parts);

        verify(streamWriter, times(parts.size())).append(any());
        verify(streamWriter).close();
        verify(writeClient).finalizeWriteStream("name");
        if (succeeds) {
            verify(writeClient).batchCommitWriteStreams(any(BatchCommitWriteStreamsRequest.class));
        } else {
            // If an error occurred while appending rows, then data should not be committed.
            verify(writeClient, never()).batchCommitWriteStreams(any(BatchCommitWriteStreamsRequest.class));
        }
    }

    @Test
    void testRunSinkQuerySucceeds() throws DescriptorValidationException, IOException, InterruptedException, ExecutionException, TimeoutException {
        testRunSinkQuery(true);
    }

    @Test
    void testRunSinkQueryFailsNoCommit() throws DescriptorValidationException, IOException, InterruptedException, ExecutionException, TimeoutException {
        testRunSinkQuery(false);
    }

    private void testRunSourceQuery(String sinkType) throws InterruptedException, IOException {
        var sinkAddress = DataAddress.Builder.newInstance()
                .type(sinkType)
                .property("customerName", TEST_CUSTOMER_NAME)
                .build();

        var queryJob = mock(Job.class);
        var tableResult = mock(TableResult.class);
        var schema = mock(Schema.class);
        var jobStatus = mock(JobStatus.class);

        when(bigQuery.create(any(JobInfo.class))).thenReturn(queryJob);

        when(queryJob.getQueryResults()).thenReturn(tableResult);
        when(tableResult.getSchema()).thenReturn(schema);
        when(schema.getFields()).thenReturn(FieldList.of(TEST_FIELDS));
        when(tableResult.iterateAll()).thenReturn(Arrays.asList(
                FieldValueList.of(TEST_ROW_1),
                FieldValueList.of(TEST_ROW_2)
        ));

        var options = mock(BigQueryOptions.class);
        when(bigQuery.getOptions()).thenReturn(options);

        var credentials = mock(OAuth2Credentials.class);
        when(options.getCredentials()).thenReturn(credentials);
        when(credentials.getAccessToken()).thenReturn(null);

        when(queryJob.waitFor()).thenReturn(queryJob);
        when(queryJob.getStatus()).thenReturn(jobStatus);
        when(jobStatus.getError()).thenReturn(null);

        var receivedParts = bigQueryService.runSourceQuery(TEST_QUERY, sinkAddress);

        verify(credentials).refreshIfExpired();
        verify(bigQuery).create(any(JobInfo.class));
        verify(queryJob).waitFor();

        var rows = Arrays.asList(TEST_ROW_1, TEST_ROW_2);
        var expectedParts = Arrays.asList(getPart(TEST_FIELDS, rows));

        var receivedList = receivedParts.toList();
        assertThat(receivedList.size()).isEqualTo(expectedParts.size());
        for (int i = 0; i < receivedList.size(); i++) {
            assertThat(receivedList.get(i)).isEqualTo(expectedParts.get(i));
        }
    }

    @Test
    void testRunSourceQueryWithBigQuerySink() throws InterruptedException, IOException {
        testRunSourceQuery("BigQueryData");
    }

    @Test
    void testRunSourceQueryWithStandardSink() throws InterruptedException, IOException {
        testRunSourceQuery("StandardSink");
    }

    private JSONObject buildRecord(List<Field> fields, List<FieldValue> values) {
        var record = new JSONObject();
        for (int i = 0; i < fields.size(); i++) {
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
}
