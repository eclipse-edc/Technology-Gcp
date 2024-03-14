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
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValue.Attribute;
import com.google.cloud.bigquery.LegacySQLTypeName;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsRequest;
import com.google.cloud.bigquery.storage.v1.BatchCommitWriteStreamsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.FinalizeWriteStreamResponse;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource.Part;
import org.eclipse.edc.gcp.bigquery.BigQueryPart;
import org.eclipse.edc.gcp.bigquery.BigQueryTarget;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.types.TypeManager;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.eclipse.edc.gcp.bigquery.service.Asserts.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

class BigQuerySinkServiceImplTest {
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
    private final BigQueryTarget target = new BigQueryTarget(TEST_PROJECT, TEST_DATASET, TEST_TABLE);
    private Random random;
    private BigQuerySinkServiceImpl bqSinkService;
    private Monitor monitor = mock();
    private GcpConfiguration gcpConfiguration = mock();
    private BigQueryWriteClient writeClient = mock();
    private Table table = mock();
    private TypeManager typeManager = mock();
    private JsonStreamWriter streamWriter = mock();

    @BeforeEach
    void setUp() {
        reset(gcpConfiguration);
        reset(monitor);
        reset(writeClient);
        reset(table);
        reset(typeManager);
        reset(streamWriter);

        bqSinkService = BigQuerySinkServiceImpl.Builder.newInstance(gcpConfiguration, target, monitor)
                .writeClient(writeClient)
                .streamWriter(streamWriter)
                .build();

        // New seed for each test.
        random = new Random();
    }

    @Test
    void testRunSinkQuerySucceeds() throws DescriptorValidationException, IOException, InterruptedException, ExecutionException, TimeoutException {
        testRunSinkQuery(true);
    }

    @Test
    void testRunSinkQueryFailsNoCommit() throws DescriptorValidationException, IOException, InterruptedException, ExecutionException, TimeoutException {
        testRunSinkQuery(false);
    }

    @Test
    void testCircularBufferWithContent() {
        var buffer = new BigQuerySinkServiceImpl.CircularBuffer(256);

        for (var i = 0; i < 1000; ++i) {
            var availableWriteCount = buffer.getAvailableWriteCount();
            var stringLength = Math.min(availableWriteCount, 37);

            var generatedString = random.ints(32, 126 + 1)
                    .limit(stringLength)
                    .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                    .toString();

            System.arraycopy(generatedString.getBytes(), 0, buffer.getBuffer(), buffer.getWriteOffset(), generatedString.length());
            buffer.write(generatedString.length());

            var availableReadCount = buffer.getAvailableReadCount();
            assertThat(availableReadCount).isEqualTo(generatedString.length());

            var fetchedString = new String(buffer.getBuffer(), buffer.getReadOffset(), availableReadCount);
            assertThat(fetchedString).isEqualTo(generatedString);

            buffer.read(fetchedString.length());
        }
    }

    @Test
    void testCircularBufferWithoutContent() {
        var buffer = new BigQuerySinkServiceImpl.CircularBuffer(256);
        var min = 13;

        for (var i = 0; i < 1000; ++i) {
            var availableWriteCount = buffer.getAvailableWriteCount();

            var randomCount = min + Math.abs(random.nextInt(123));
            var used = Math.min(availableWriteCount, randomCount);
            buffer.write(used);
            var availableReadCount = buffer.getAvailableReadCount();
            assertThat(availableReadCount).isEqualTo(used);
            buffer.read(availableReadCount);
        }
    }

    @Test
    void testCircularBufferWithoutContentMultipleWrite() {
        var buffer = new BigQuerySinkServiceImpl.CircularBuffer(256);
        var min = 13;

        for (var i = 0; i < 1000; ++i) {
            var sum = 0;
            var writeCount = 1 + Math.abs(random.nextInt(7));

            for (var j = 0; j < writeCount; ++j) {
                var availableWriteCount = buffer.getAvailableWriteCount();
                var randomCount = min + Math.abs(random.nextInt(31));
                var used = Math.min(availableWriteCount, randomCount);

                buffer.write(used);
                sum += used;
            }

            var availableReadCount = buffer.getAvailableReadCount();
            assertThat(availableReadCount).isEqualTo(sum);
            buffer.read(availableReadCount);
        }
    }

    @Test
    void testCircularBufferFails() {
        for (var i = 0; i < 4; ++i) {
            var size = 16 + Math.abs(random.nextInt(32768));
            var buffer = new BigQuerySinkServiceImpl.CircularBuffer(size);

            var readException = assertThrows(GcpException.class, () -> buffer.read(1));
            assertThat(readException.getMessage()).isEqualTo("BigQuery buffer cannot read 1 bytes, max count is 0");

            var maxWrite = buffer.getAvailableWriteCount();
            assertThat(maxWrite).isEqualTo(size - 1);

            var writeException = assertThrows(GcpException.class, () -> buffer.write(maxWrite + 1));
            assertThat(writeException.getMessage()).isEqualTo("BigQuery buffer cannot write " + (maxWrite + 1) + " bytes, max count is " + maxWrite);

            var writeCount = maxWrite / 3;
            buffer.write(writeCount);

            var maxRead = buffer.getAvailableReadCount();
            assertThat(maxRead).isEqualTo(writeCount);
            readException = assertThrows(GcpException.class, () -> buffer.read(maxRead + 1));
            assertThat(readException.getMessage()).isEqualTo("BigQuery buffer cannot read " + (maxRead + 1) + " bytes, max count is " + maxRead);
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
        executorService.schedule(() -> bqSinkService.testAppendSignal(), 2, TimeUnit.SECONDS);

        bqSinkService.runSinkQuery(parts);

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
}
