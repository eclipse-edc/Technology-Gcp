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
import org.eclipse.edc.gcp.bigquery.service.BigQueryServiceSchema;
import org.eclipse.edc.gcp.bigquery.service.BigQuerySourceService;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.util.ArrayList;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BigQueryDataSourceTest {
    private BigQuerySourceService bqSourceService = mock();
    private Monitor monitor = mock();
    private static final String REQUEST_ID = "request-id";
    private static final String TEST_PROJECT = "test-project";
    private static final String TEST_DATASET = "test-dataset";
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_CUSTOMER_NAME = "customer-name";
    private static final String TEST_QUERY_NO_PARAMS = "select * from " + TEST_TABLE;
    private static final String TEST_SINK_SERVICE_ACCOUNT_NAME = "sinkAccount";
    private static final String TEST_PARAM_NAME = "argument";
    private static final String TEST_PARAM_VALUE = "argValue";
    private static final String TEST_QUERY_WITH_PARAMS = "select * from " + TEST_TABLE + " where column=@" + TEST_PARAM_NAME;
    private static final String TEST_ROW1_JSON = "\"column1\": \"data1_row1\", \"column2\": \"data2_row1\"";
    private static final String TEST_ROW2_JSON = "\"column1\": \"data1_row2\", \"column2\": \"data2_row2\"";

    @BeforeEach
    void setup() {
        reset(bqSourceService);
        reset(monitor);
    }

    @Test
    void testOpenStreamWithoutParams() throws InterruptedException {
        var params = BigQueryRequestParams.Builder.newInstance()
                .project(TEST_PROJECT)
                .dataset(TEST_DATASET)
                .table(TEST_TABLE)
                .query(TEST_QUERY_NO_PARAMS)
                .sinkAddress(getDestinationDataAddress())
                .build();

        var dataSource = BigQueryDataSource.Builder.newInstance()
                .monitor(monitor)
                .requestId(REQUEST_ID)
                .params(params)
                .sourceService(bqSourceService)
                .build();

        var parts = new ArrayList<Part>();
        parts.add(new BigQueryPart("allRows", new ByteArrayInputStream(TEST_ROW1_JSON.getBytes())));
        parts.add(new BigQueryPart("allRows", new ByteArrayInputStream(TEST_ROW2_JSON.getBytes())));

        when(bqSourceService.runSourceQuery(TEST_QUERY_NO_PARAMS, null, params.getSinkAddress())).thenReturn(parts.stream());

        var response = dataSource.openPartStream();
        assertThat(response.succeeded()).isTrue();
        assertThat(response.getContent().toList()).isEqualTo(parts);

        verify(bqSourceService).runSourceQuery(TEST_QUERY_NO_PARAMS, null, params.getSinkAddress());
    }

    private static DataAddress.Builder getDefaultAddressBuilder() {
        return DataAddress.Builder.newInstance()
            .type(BigQueryServiceSchema.BIGQUERY_DATA)
            .property(BigQueryServiceSchema.PROJECT, TEST_PROJECT)
            .property(BigQueryServiceSchema.DATASET, TEST_DATASET)
            .property(BigQueryServiceSchema.TABLE, TEST_TABLE)
            .property(BigQueryServiceSchema.CUSTOMER_NAME, TEST_CUSTOMER_NAME)
            .property(BigQueryServiceSchema.SERVICE_ACCOUNT_NAME, TEST_SINK_SERVICE_ACCOUNT_NAME);
    }

    private static DataAddress getDestinationDataAddress() {
        return getDefaultAddressBuilder().build();
    }

    private static DataAddress getDestinationDataAddress(Map<String, String> queryParams) {
        var builder = getDefaultAddressBuilder();

        queryParams.forEach((k, v) -> builder.property(k, v));

        return builder.build();
    }
}
