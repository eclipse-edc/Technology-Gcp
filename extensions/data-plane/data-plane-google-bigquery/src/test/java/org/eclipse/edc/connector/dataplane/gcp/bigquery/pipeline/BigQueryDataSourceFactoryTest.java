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

import org.eclipse.edc.connector.dataplane.gcp.bigquery.params.BigQueryRequestParamsProviderImpl;
import org.eclipse.edc.gcp.bigquery.service.BigQueryServiceSchema;
import org.eclipse.edc.gcp.bigquery.service.BigQuerySourceService;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;

public class BigQueryDataSourceFactoryTest {
    private static final String TEST_PROJECT = "test-project";
    private static final String TEST_DATASET = "test-dataset";
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_QUERY = "select * from " + TEST_TABLE + ";";
    private static final String TEST_OTHER_TYPE = "AnotherDataAddressType";
    private static final String TEST_REQUEST_ID = "request-id";
    private static final String TEST_PROCESS_ID = "process-id";
    private static final String TEST_CUSTOMER_NAME = "customer-name";
    private static final String TEST_SINK_SERVICE_ACCOUNT_NAME = "sinkAccount";
    private static final String TEST_PARAM_NAME = "argument";
    private static final String TEST_PARAM_VALUE = "argValue";
    private final ExecutorService executionPool = Executors.newFixedThreadPool(2);
    private GcpConfiguration gcpConfiguration = mock();
    private TypeManager typeManager = mock();
    private BigQuerySourceService bqSourceService = mock();
    private Monitor monitor = mock();
    private IamService iamService = mock();

    @BeforeEach
    void setup() {
        reset(bqSourceService);
        reset(monitor);
        reset(typeManager);
        reset(gcpConfiguration);
        reset(iamService);
    }

    @Test
    void testCanHandle() {
        var provider = new BigQueryRequestParamsProviderImpl();
        var factory = BigQueryDataSourceFactory.Builder.newInstance(gcpConfiguration, monitor, provider, typeManager, executionPool, iamService)
                  .build();

        var bqDataFlowRequest = getDataFlowRequest(BigQueryServiceSchema.BIGQUERY_DATA);

        assertThat(factory.canHandle(bqDataFlowRequest)).isTrue();

        var otherDataFlowRequest = getDataFlowRequest(TEST_OTHER_TYPE);

        assertThat(factory.canHandle(otherDataFlowRequest)).isFalse();
    }

    @Test
    void testValidateRequest() {
        // TODO add tests if validateRequest body is implemented with specific tests.
    }

    @Test
    void testCreateSource() {
        var provider = new BigQueryRequestParamsProviderImpl();
        var factory = BigQueryDataSourceFactory.Builder.newInstance(gcpConfiguration, monitor, provider, typeManager, executionPool, iamService)
                .sourceService(bqSourceService)
                .build();

        var bqDataFlowRequest = getDataFlowRequest(BigQueryServiceSchema.BIGQUERY_DATA);

        try (var bqSource = factory.createSource(bqDataFlowRequest)) {
            assertThat(bqSource).isNotNull();
        } catch (Exception e) {
            assertThat(true).isFalse();
            System.out.println("Doesn't enter here");
        } finally {
            System.out.println("Resource clean-up");
        }

        var otherDataFlowRequest = getDataFlowRequest(TEST_OTHER_TYPE);

        var exception = assertThrows(GcpException.class, () -> factory.createSource(otherDataFlowRequest));
        assertThat(exception.getMessage()).isEqualTo("BigQuery Data Source cannot create source for request type " + TEST_OTHER_TYPE);
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
}
