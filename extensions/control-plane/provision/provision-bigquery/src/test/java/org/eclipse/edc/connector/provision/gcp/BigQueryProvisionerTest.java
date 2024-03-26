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

package org.eclipse.edc.connector.provision.gcp;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.gcp.bigquery.BigQueryTarget;
import org.eclipse.edc.gcp.bigquery.service.BigQueryFactory;
import org.eclipse.edc.gcp.bigquery.service.BigQueryService;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

class BigQueryProvisionerTest {
    private static final String TEST_PROJECT = "test-project";
    private static final String TEST_DATASET = "test-dataset";
    private static final String TEST_TABLE = "test-table";
    private static final String RESOURCE_ID = "mandatory-id";
    private static final String RESOURCE_DEFINITION_ID = "resource-definition-id";
    private static final String TRANSFER_ID = "transfer-id";
    private static final String CUSTOMER_NAME = "customer-name";
    private static final String RESOURCE_NAME = "resource-name";
    private static final BigQueryTarget TEST_TARGET = new BigQueryTarget(TEST_PROJECT, TEST_DATASET, TEST_TABLE);
    private final ObjectMapper objectMapper = new ObjectMapper();
    private Monitor monitor = mock();
    private GcpConfiguration gcpConfiguration = mock();
    private BigQuery bigQuery = mock();

    @BeforeEach
    void setUp() {
        reset(gcpConfiguration);
        reset(monitor);
        reset(bigQuery);
    }

    @Test
    void testCanProvisionTrue() {
        var bigQueryProvisioner = BigQueryProvisioner.Builder.newInstance(gcpConfiguration, monitor)
                .build();

        var resourceDefinition = BigQueryResourceDefinition.Builder.newInstance()
                .id(RESOURCE_ID)
                .property(BigQueryService.PROJECT, TEST_PROJECT)
                .property(BigQueryService.DATASET, TEST_DATASET)
                .property(BigQueryService.TABLE, TEST_TABLE)
                .build();

        assertThat(bigQueryProvisioner.canProvision(resourceDefinition)).isTrue();
    }

    @Test
    void testCanProvisionFalse() {
        var bigQueryProvisioner = BigQueryProvisioner.Builder.newInstance(gcpConfiguration, monitor)
                .build();

        assertThat(bigQueryProvisioner.canProvision(new ResourceDefinition() {
            @Override
            public <RD extends ResourceDefinition, B extends Builder<RD, B>> B toBuilder() {
                return null;
            }
        })).isFalse();
    }

    @Test
    void testCanDeprovisionTrue() {
        var bigQueryProvisioner = BigQueryProvisioner.Builder.newInstance(gcpConfiguration, monitor)
                .build();

        var provisionedResource = BigQueryProvisionedResource.Builder.newInstance()
                .id(RESOURCE_ID)
                .transferProcessId(TRANSFER_ID)
                .resourceDefinitionId(RESOURCE_DEFINITION_ID)
                .resourceName(RESOURCE_NAME)
                .build();

        assertThat(bigQueryProvisioner.canDeprovision(provisionedResource)).isTrue();
    }

    @Test
    void testCanDeprovisionFalse() {
        var bigQueryProvisioner = BigQueryProvisioner.Builder.newInstance(gcpConfiguration, monitor)
                .build();

        assertThat(bigQueryProvisioner.canDeprovision(new ProvisionedResource() {
        })).isFalse();
    }

    @Test
    void provisionSuccess() throws InterruptedException, ExecutionException {
        var bigQueryProvisioner = BigQueryProvisioner.Builder.newInstance(gcpConfiguration, monitor)
                .bqFactory(new BigQueryFactoryTest())
                .build();

        var resourceDefinition = BigQueryResourceDefinition.Builder.newInstance()
                .id(RESOURCE_ID)
                .transferProcessId(TRANSFER_ID)
                .property(BigQueryService.PROJECT, TEST_PROJECT)
                .property(BigQueryService.DATASET, TEST_DATASET)
                .property(BigQueryService.TABLE, TEST_TABLE)
                .property(BigQueryService.CUSTOMER_NAME, CUSTOMER_NAME)
                .build();

        var policy = Policy.Builder.newInstance().build();

        var table = mock(Table.class);
        when(table.exists()).thenReturn(true);
        when(bigQuery.getTable(TEST_TARGET.getTableId())).thenReturn(table);

        var response = bigQueryProvisioner.provision(resourceDefinition, policy).join().getContent();
        assertThat(response.getResource()).isInstanceOfSatisfying(BigQueryProvisionedResource.class, resource -> {
            assertThat(resource.getId()).isEqualTo(RESOURCE_ID);
            assertThat(resource.getTransferProcessId()).isEqualTo(TRANSFER_ID);
            assertThat(resource.getProject()).isEqualTo(TEST_PROJECT);
            assertThat(resource.getDataset()).isEqualTo(TEST_DATASET);
            assertThat(resource.getTable()).isEqualTo(TEST_TABLE);
            assertThat(resource.hasToken()).isTrue();
            assertThat(resource.getCustomerName()).isEqualTo(CUSTOMER_NAME);
        });
    }

    @Test
    void provisionFailsIfTableDoesntExist() throws InterruptedException, ExecutionException {
        var bigQueryProvisioner = BigQueryProvisioner.Builder.newInstance(gcpConfiguration, monitor)
                .bqFactory(new BigQueryFactoryTest())
                .build();

        var resourceDefinition = BigQueryResourceDefinition.Builder.newInstance()
                .id(RESOURCE_ID)
                .transferProcessId(TRANSFER_ID)
                .property(BigQueryService.PROJECT, TEST_PROJECT)
                .property(BigQueryService.DATASET, TEST_DATASET)
                .property(BigQueryService.TABLE, TEST_TABLE)
                .property(BigQueryService.CUSTOMER_NAME, CUSTOMER_NAME)
                .build();

        var policy = Policy.Builder.newInstance().build();

        var table = mock(Table.class);
        when(table.exists()).thenReturn(false);
        when(bigQuery.getTable(TEST_TARGET.getTableId())).thenReturn(table);

        var response = bigQueryProvisioner.provision(resourceDefinition, policy).join();
        assertThat(response.failed()).isTrue();
    }

    private class BigQueryFactoryTest implements BigQueryFactory {
        public BigQuery createBigQuery(GcpConfiguration gcpConfiguration, GoogleCredentials credentials, Monitor monitor) {
            return bigQuery;
        }

        public BigQuery createBigQuery(GcpConfiguration gcpConfiguration, String serviceAccountName, Monitor monitor) throws IOException {
            return bigQuery;
        }

        public BigQuery createBigQuery(GcpConfiguration gcpConfiguration, Monitor monitor) throws IOException {
            return bigQuery;
        }
    }
}
