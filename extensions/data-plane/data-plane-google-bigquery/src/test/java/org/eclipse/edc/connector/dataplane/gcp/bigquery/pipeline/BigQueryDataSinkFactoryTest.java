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
import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class BigQueryDataSinkFactoryTest {
    private static final String TEST_PROJECT = "test-project";
    private static final String TEST_DATASET = "test-dataset";
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_REQUEST_ID = "request-id";
    private static final String TEST_PROCESS_ID = "process-id";
    private static final String TEST_CUSTOMER_NAME = "customer-name";
    private static final String TEST_SINK_SERVICE_ACCOUNT_NAME = "sinkAccount";
    private final BigQueryConfiguration bigQueryConfiguration = new BigQueryConfiguration(null, null, "testEndpoint", 0);
    private final TypeManager typeManager = mock();
    private final Monitor monitor = mock();
    private final ExecutorService executorService = mock();
    private final Vault vault = mock();
    private final IamService iamService = mock();

    @BeforeEach
    void setup() {
        when(monitor.withPrefix(any(String.class))).thenReturn(monitor);
    }

    @Test
    void testCreateSink() {
        var provider = new BigQueryRequestParamsProvider();
        var factory = new BigQueryDataSinkFactory(bigQueryConfiguration, executorService, monitor, vault, typeManager, provider, iamService);

        var bqDataFlowRequest = getDataFlowRequest(BigQueryServiceSchema.BIGQUERY_DATA);

        var bqSink = factory.createSink(bqDataFlowRequest);
        assertThat(bqSink).isNotNull();
    }

    private DataFlowStartMessage getDataFlowRequest(String type) {
        var dataAddress = DataAddress.Builder.newInstance()
                .type(type)
                .property(BigQueryServiceSchema.PROJECT, TEST_PROJECT)
                .property(BigQueryServiceSchema.DATASET, TEST_DATASET)
                .property(BigQueryServiceSchema.TABLE, TEST_TABLE)
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
}
