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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValue.Attribute;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import org.eclipse.edc.connector.dataplane.gcp.bigquery.params.BigQueryRequestParams;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource.Part;
import org.eclipse.edc.gcp.bigquery.BigQueryConfiguration;
import org.eclipse.edc.gcp.bigquery.BigQueryPart;
import org.eclipse.edc.gcp.bigquery.BigQueryTarget;
import org.eclipse.edc.spi.monitor.Monitor;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BigQueryDataSinkTest {
    private Monitor monitor = mock();
    private ExecutorService executorService = mock();
    private static final String TEST_PROJECT = "test-project";
    private static final String TEST_DATASET = "test-dataset";
    private static final String TEST_TABLE = "test-table";
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
    private static final String REQUEST_ID = "request-id";
    private static final String TEST_ROW1_JSON = "\"column1\": \"data1_row1\", \"column2\": \"data2_row1\"";
    private static final String TEST_ROW2_JSON = "\"column1\": \"data1_row2\", \"column2\": \"data2_row2\"";
    private static final List<Part> TEST_PARTS = Arrays.asList(
            new BigQueryPart("allRows", new ByteArrayInputStream(TEST_ROW1_JSON.getBytes())),
            new BigQueryPart("allRows", new ByteArrayInputStream(TEST_ROW2_JSON.getBytes()))
    );
    private BigQueryRequestParams params;
    private BigQueryDataSink dataSink;
    private final String testTokenValue = "test-access-token";
    private final long testTokenExpirationTime = 433454334;
    private final BigQueryTarget target = new BigQueryTarget(TEST_PROJECT, TEST_DATASET, TEST_TABLE);
    private final AccessToken credentialAccessToken = AccessToken.newBuilder()
            .setTokenValue(testTokenValue)
            .setExpirationTime(new Date(testTokenExpirationTime))
            .build();
    private BigQueryWriteClient writeClient = mock();
    private JsonStreamWriter streamWriter = mock();
    private GoogleCredentials googleCredentials = mock();

    @BeforeEach
    void setup() {
        reset(monitor);
        reset(executorService);
        reset(writeClient);
        reset(streamWriter);
        reset(googleCredentials);

        when(googleCredentials.createScoped(any(String[].class))).thenReturn(googleCredentials);
        when(googleCredentials.getAccessToken()).thenReturn(credentialAccessToken);

        when(monitor.withPrefix(any(String.class))).thenReturn(monitor);

        params = BigQueryRequestParams.Builder.newInstance()
                .project(TEST_PROJECT)
                .dataset(TEST_DATASET)
                .table(TEST_TABLE)
                .build();

        dataSink = BigQueryDataSink.Builder.newInstance()
                .monitor(monitor)
                .requestId(REQUEST_ID)
                .writeClient(writeClient)
                .streamWriter(streamWriter)
                .credentials(googleCredentials)
                .executorService(executorService)
                .bigQueryTarget(target)
                .objectMapper(new ObjectMapper())
                .configuration(new BigQueryConfiguration(null))
                .build();
    }

    @Test
    void testTransferPartsSinglePageSucceeds() throws DescriptorValidationException, IOException, InterruptedException, ExecutionException, TimeoutException {
        var parts = new ArrayList<Part>();
        var partsRows =  Arrays.asList(
                Arrays.asList(TEST_ROW_1, TEST_ROW_2));

        parts.add(getPartFromPages(TEST_FIELDS, partsRows));

        var rowsCount = partsRows.stream().mapToLong(o -> o.size()).sum();

        ApiFuture<AppendRowsResponse> future = mock();
        AppendRowsResponse response = mock();
        when(future.isDone()).thenReturn(true);
        when(future.cancel(anyBoolean())).thenReturn(false);
        when(future.isCancelled()).thenReturn(false);
        doNothing().when(future).addListener(any(Runnable.class), any(Executor.class));
        when(future.get()).thenReturn(response);
        when(future.get(anyLong(), any())).thenReturn(response);
        when(streamWriter.append(any())).thenReturn(future);

        when(streamWriter.isClosed()).thenReturn(false);
        when(streamWriter.getStreamName()).thenReturn("name");
        when(writeClient.finalizeWriteStream(streamWriter.getStreamName())).thenReturn(
                FinalizeWriteStreamResponse.newBuilder().setRowCount(rowsCount).build());

        var commitResponse = mock(BatchCommitWriteStreamsResponse.class);
        when(commitResponse.hasCommitTime()).thenReturn(true);
        when(commitResponse.getStreamErrorsList()).thenReturn(Arrays.asList());
        when(writeClient.batchCommitWriteStreams(any(BatchCommitWriteStreamsRequest.class))).thenReturn(commitResponse);

        var executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.schedule(() -> dataSink.testAppendSignal(), 2, TimeUnit.SECONDS);

        dataSink.transferParts(parts);

        verify(streamWriter, times(parts.size())).append(any());
        verify(streamWriter).close();
        verify(writeClient).finalizeWriteStream("name");
        verify(writeClient).batchCommitWriteStreams(any(BatchCommitWriteStreamsRequest.class));
    }

    @Test
    void testTransferPartsMultiPageSucceeds() throws DescriptorValidationException, IOException, InterruptedException, ExecutionException, TimeoutException {
        var parts = new ArrayList<Part>();
        var partsRows =  Arrays.asList(
                Arrays.asList(TEST_ROW_1, TEST_ROW_2, TEST_ROW_1, TEST_ROW_2),
                Arrays.asList(TEST_ROW_1, TEST_ROW_2));

        parts.add(getPartFromPages(TEST_FIELDS, partsRows));

        var rowsCount = partsRows.stream().mapToLong(o -> o.size()).sum();

        ApiFuture<AppendRowsResponse> future = mock();
        AppendRowsResponse response = mock();
        when(future.isDone()).thenReturn(true);
        when(future.cancel(anyBoolean())).thenReturn(false);
        when(future.isCancelled()).thenReturn(false);
        doNothing().when(future).addListener(any(Runnable.class), any(Executor.class));
        when(future.get()).thenReturn(response);
        when(future.get(anyLong(), any())).thenReturn(response);
        when(streamWriter.append(any())).thenReturn(future);

        when(streamWriter.isClosed()).thenReturn(false);
        when(streamWriter.getStreamName()).thenReturn("name");
        when(writeClient.finalizeWriteStream(streamWriter.getStreamName())).thenReturn(
                FinalizeWriteStreamResponse.newBuilder().setRowCount(rowsCount).build());

        var commitResponse = mock(BatchCommitWriteStreamsResponse.class);
        when(commitResponse.hasCommitTime()).thenReturn(true);
        when(commitResponse.getStreamErrorsList()).thenReturn(Arrays.asList());
        when(writeClient.batchCommitWriteStreams(any(BatchCommitWriteStreamsRequest.class))).thenReturn(commitResponse);
        when(writeClient.awaitTermination(anyInt(), any(TimeUnit.class))).thenReturn(true);

        var executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.schedule(() -> dataSink.testAppendSignal(), 2, TimeUnit.SECONDS);
        executorService.schedule(() -> dataSink.testAppendSignal(), 3, TimeUnit.SECONDS);

        dataSink.transferParts(parts);

        verify(streamWriter, times(partsRows.size())).append(any());
        verify(streamWriter).close();
        verify(writeClient).finalizeWriteStream("name");
        verify(writeClient).batchCommitWriteStreams(any(BatchCommitWriteStreamsRequest.class));
    }

    @Test
    void testTransferPartsFailsNoCommit() throws DescriptorValidationException, IOException, InterruptedException, ExecutionException, TimeoutException {
        var parts = new ArrayList<Part>();
        var partsRows =  Arrays.asList(
                Arrays.asList(TEST_ROW_1, TEST_ROW_2));

        parts.add(getPartFromPages(TEST_FIELDS, partsRows));

        var rowsCount = partsRows.stream().mapToLong(o -> o.size()).sum();

        when(streamWriter.append(any())).thenThrow(new IOException());
        when(streamWriter.isClosed()).thenReturn(false);
        when(streamWriter.getStreamName()).thenReturn("name");
        when(writeClient.finalizeWriteStream(streamWriter.getStreamName())).thenReturn(
                FinalizeWriteStreamResponse.newBuilder().setRowCount(rowsCount).build());

        var commitResponse = mock(BatchCommitWriteStreamsResponse.class);
        when(commitResponse.hasCommitTime()).thenReturn(true);
        when(commitResponse.getStreamErrorsList()).thenReturn(Arrays.asList());
        when(writeClient.batchCommitWriteStreams(any(BatchCommitWriteStreamsRequest.class))).thenReturn(commitResponse);

        var executorService = Executors.newSingleThreadScheduledExecutor();
        executorService.schedule(() -> dataSink.testAppendSignal(), 2, TimeUnit.SECONDS);

        dataSink.transferParts(parts);

        verify(streamWriter, times(parts.size())).append(any());
        verify(streamWriter).close();
        verify(writeClient).finalizeWriteStream("name");
        // If an error occurred while appending rows, then data should not be committed.
        verify(writeClient, never()).batchCommitWriteStreams(any(BatchCommitWriteStreamsRequest.class));
    }

    private JSONObject buildRecord(List<Field> fields, List<FieldValue> values) {
        var record = new JSONObject();
        for (var i = 0; i < fields.size(); i++) {
            record.put(fields.get(i).getName(), values.get(i).getValue());
        }

        return record;
    }

    private BigQueryPart getPartFromPages(List<Field> fieldList, List<List<List<FieldValue>>> pages) {
        var json = "";
        for (var page : pages) {
            var array = new JSONArray();
            for (var row : page) {
                array.put(buildRecord(fieldList, row));
            }
            json += array.toString();
        }

        return new BigQueryPart("allRows", new ByteArrayInputStream(
            json.getBytes()));
    }
}
