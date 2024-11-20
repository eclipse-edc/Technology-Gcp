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

import org.eclipse.edc.connector.dataplane.gcp.bigquery.params.BigQueryRequestParamsProvider;
import org.eclipse.edc.gcp.bigquery.BigQueryConfiguration;
import org.eclipse.edc.gcp.bigquery.service.BigQueryServiceSchema;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.eclipse.edc.junit.assertions.AbstractResultAssert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BigQueryDataSourceFactoryTest {
    private static final String TEST_PROJECT = "test-project";
    private static final String TEST_DATASET = "test-dataset";
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_QUERY = "select * from " + TEST_TABLE + ";";
    private static final String TEST_REQUEST_ID = "request-id";
    private static final String TEST_PROCESS_ID = "process-id";
    private static final String TEST_CUSTOMER_NAME = "customer-name";
    private static final String TEST_SINK_SERVICE_ACCOUNT_NAME = "sinkAccount";
    private final ExecutorService executionPool = Executors.newFixedThreadPool(2);
    private final GcpConfiguration gcpConfiguration = new GcpConfiguration(TEST_PROJECT, TEST_SINK_SERVICE_ACCOUNT_NAME, null, null);
    private final BigQueryConfiguration configuration = new BigQueryConfiguration(gcpConfiguration, "testEndpoint", null, 0);
    private final Monitor monitor = mock();
    private final IamService iamService = mock();
    private final BigQueryDataSourceFactory factory = new BigQueryDataSourceFactory(configuration, monitor,
            new BigQueryRequestParamsProvider(), executionPool, iamService);

    @BeforeEach
    void setup() {
        when(monitor.withPrefix(any(String.class))).thenReturn(monitor);
    }

    @Test
    void shouldValidateRequest() {
        var message = DataFlowStartMessage.Builder.newInstance()
                .processId(UUID.randomUUID().toString())
                .sourceDataAddress(getSourceDataAddress())
                .destinationDataAddress(getDestinationDataAddress())
                .build();

        var result = factory.validateRequest(message);

        assertThat(result).isSucceeded();
    }

    @Test
    void testCreateSource() {
        var bqDataFlowRequest = getDataFlowRequest(BigQueryServiceSchema.BIGQUERY_DATA);

        var bqSource = factory.createSource(bqDataFlowRequest);

        assertThat(bqSource).isNotNull();
    }

    private DataFlowStartMessage getDataFlowRequest(String type) {
        var dataAddress = DataAddress.Builder.newInstance()
                .type(type)
                .property(BigQueryServiceSchema.PROJECT, TEST_PROJECT)
                .property(BigQueryServiceSchema.DATASET, TEST_DATASET)
                .property(BigQueryServiceSchema.TABLE, TEST_TABLE)
                .property(BigQueryServiceSchema.QUERY, TEST_QUERY)
                .property(BigQueryServiceSchema.CUSTOMER_NAME, TEST_CUSTOMER_NAME)
                .property(BigQueryServiceSchema.SERVICE_ACCOUNT_NAME, TEST_SINK_SERVICE_ACCOUNT_NAME)
                .build();

        return DataFlowStartMessage.Builder.newInstance()
                .id(TEST_REQUEST_ID)
                .processId(TEST_PROCESS_ID)
                .sourceDataAddress(dataAddress)
                .destinationDataAddress(dataAddress)
                .build();
    }


    private DataAddress getSourceDataAddress() {
        return DataAddress.Builder.newInstance()
                .type(BigQueryServiceSchema.BIGQUERY_DATA)
                .property(BigQueryServiceSchema.QUERY, "select * from table;")
                .build();
    }

    private DataAddress getDestinationDataAddress() {
        return DataAddress.Builder.newInstance()
                .type(BigQueryServiceSchema.BIGQUERY_DATA)
                .property(BigQueryServiceSchema.DATASET, TEST_DATASET)
                .property(BigQueryServiceSchema.TABLE, TEST_TABLE)
                .build();
    }
}
