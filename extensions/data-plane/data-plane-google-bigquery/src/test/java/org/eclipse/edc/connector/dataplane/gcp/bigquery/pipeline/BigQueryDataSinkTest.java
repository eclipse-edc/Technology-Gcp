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

import org.eclipse.edc.connector.dataplane.gcp.bigquery.params.BigQueryRequestParams;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource.Part;
import org.eclipse.edc.gcp.bigquery.BigQueryPart;
import org.eclipse.edc.gcp.bigquery.service.BigQuerySinkService;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;

public class BigQueryDataSinkTest {
    private BigQuerySinkService bqSinkService = mock();
    private Monitor monitor = mock();
    private ExecutorService executorService = mock();
    private static final String REQUEST_ID = "request-id";
    private static final String TEST_PROJECT = "test-project";
    private static final String TEST_DATASET = "test-dataset";
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_ROW1_JSON = "\"column1\": \"data1_row1\", \"column2\": \"data2_row1\"";
    private static final String TEST_ROW2_JSON = "\"column1\": \"data1_row2\", \"column2\": \"data2_row2\"";
    private static final List<Part> TEST_PARTS = Arrays.asList(
        new BigQueryPart("allRows", new ByteArrayInputStream(TEST_ROW1_JSON.getBytes())),
        new BigQueryPart("allRows", new ByteArrayInputStream(TEST_ROW2_JSON.getBytes()))
    );
    private BigQueryRequestParams params;
    private BigQueryDataSink dataSink;

    @BeforeEach
    void setup() {
        reset(bqSinkService);
        reset(monitor);
        reset(executorService);

        params = BigQueryRequestParams.Builder.newInstance()
                .project(TEST_PROJECT)
                .dataset(TEST_DATASET)
                .table(TEST_TABLE)
                .build();

        dataSink = BigQueryDataSink.Builder.newInstance()
                .monitor(monitor)
                .requestId(REQUEST_ID)
                .sinkService(bqSinkService)
                .executorService(executorService)
                .build();
    }

    @Test
    void testTransferPartsSucceeds() {
        var response = dataSink.transferParts(TEST_PARTS);
        assertThat(response.succeeded()).isTrue();

        verify(bqSinkService).runSinkQuery(TEST_PARTS);
    }

    @Test
    void testTransferPartsFails() {
        doThrow(new GcpException("Error while running the sink query")).when(bqSinkService).runSinkQuery(TEST_PARTS);

        var response = dataSink.transferParts(TEST_PARTS);
        assertThat(response.failed()).isTrue();
        assertThat(response.getFailureDetail()).isEqualTo("BigQuery Sink error writing data to the table");

        verify(bqSinkService).runSinkQuery(TEST_PARTS);
    }
}
