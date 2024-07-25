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

package org.eclipse.edc.connector.dataplane.gcp.bigquery.params;

import org.eclipse.edc.gcp.bigquery.service.BigQueryServiceSchema;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class BigQueryRequestParamsProviderTest {
    private BigQueryRequestParamsProvider provider;
    private static final String TEST_PROJECT = "test-project";
    private static final String TEST_DATASET = "test-dataset";
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_QUERY = "select * from " + TEST_TABLE;
    private static final String TEST_SERVICE_ACCOUNT_NAME = "service-account";
    private static final String TEST_CUSTOMER_NAME = "customer_name";

    @BeforeEach
    void setup() {
        provider = new BigQueryRequestParamsProvider();
    }

    @Test
    void testProvideSinkParams() {
        var dataFlowMessage = DataFlowStartMessage.Builder.newInstance()
                .id("requestId")
                .sourceDataAddress(getSouorceDataAddress())
                .destinationDataAddress(getDestinationDataAddress())
                .processId("processId")
                .build();
        var requestParams = provider.provideSinkParams(dataFlowMessage);

        assertThat(requestParams.getProject()).isEqualTo(TEST_PROJECT);
        assertThat(requestParams.getDataset()).isEqualTo(TEST_DATASET);
        assertThat(requestParams.getTable()).isEqualTo(TEST_TABLE);
        assertThat(requestParams.getServiceAccountName()).isEqualTo(TEST_SERVICE_ACCOUNT_NAME);
        assertThat(requestParams.getSinkAddress()).isNull();
    }

    @Test
    void testProvideSourceParams() {
        var dataFlowMessage = DataFlowStartMessage.Builder.newInstance()
                .id("requestId")
                .sourceDataAddress(getSouorceDataAddress())
                .destinationDataAddress(getDestinationDataAddress())
                .processId("processId")
                .build();
        var requestParams = provider.provideSourceParams(dataFlowMessage);

        assertThat(requestParams.getProject()).isEqualTo(TEST_PROJECT);
        assertThat(requestParams.getDataset()).isEqualTo(TEST_DATASET);
        assertThat(requestParams.getTable()).isEqualTo(TEST_TABLE);
        assertThat(requestParams.getQuery()).isEqualTo(TEST_QUERY);
        assertThat(requestParams.getServiceAccountName()).isEqualTo(TEST_SERVICE_ACCOUNT_NAME);
        assertThat(requestParams.getSinkAddress()).usingRecursiveComparison()
                .isEqualTo(getDestinationDataAddress());
    }

    private static DataAddress getSouorceDataAddress() {
        return DataAddress.Builder.newInstance()
              .type(BigQueryServiceSchema.BIGQUERY_DATA)
              .property(BigQueryServiceSchema.PROJECT, TEST_PROJECT)
              .property(BigQueryServiceSchema.DATASET, TEST_DATASET)
              .property(BigQueryServiceSchema.TABLE, TEST_TABLE)
              .property(BigQueryServiceSchema.QUERY, TEST_QUERY)
              .property(BigQueryServiceSchema.CUSTOMER_NAME, TEST_CUSTOMER_NAME)
              .property(BigQueryServiceSchema.SERVICE_ACCOUNT_NAME, TEST_SERVICE_ACCOUNT_NAME)
              .build();
    }

    private static DataAddress getDestinationDataAddress() {
        return DataAddress.Builder.newInstance()
                .type(BigQueryServiceSchema.BIGQUERY_DATA)
                .property(BigQueryServiceSchema.PROJECT, TEST_PROJECT)
                .property(BigQueryServiceSchema.DATASET, TEST_DATASET)
                .property(BigQueryServiceSchema.TABLE, TEST_TABLE)
                .property(BigQueryServiceSchema.CUSTOMER_NAME, TEST_CUSTOMER_NAME)
                .property(BigQueryServiceSchema.SERVICE_ACCOUNT_NAME, TEST_SERVICE_ACCOUNT_NAME)
        .build();
    }
}
