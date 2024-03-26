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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionResponse;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.gcp.bigquery.BigQueryTarget;
import org.eclipse.edc.gcp.bigquery.service.BigQueryFactory;
import org.eclipse.edc.gcp.bigquery.service.BigQueryService;
import org.eclipse.edc.gcp.common.GcpAccessToken;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.gcp.common.GcpServiceAccount;
import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.StatusResult;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static com.google.protobuf.util.Timestamps.fromMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class BigQueryProvisionerTest {
    private static final String TEST_PROJECT = "test-project";
    private static final String TEST_DATASET = "test-dataset";
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_SERVICE_ACCOUNT_NAME = "edc_test_acccount";
    private static final String TEST_EMAIL = TEST_SERVICE_ACCOUNT_NAME + "@emailtest.edc";
    private static final String TEST_DESCRIPTION = "service account for EDC test";
    private static final String TEST_TOKEN = "fdsgfdhgbrty456ghtbrfrdfgvfchfh";
    private static final String RESOURCE_ID = "mandatory-id";
    private static final String RESOURCE_DEFINITION_ID = "resource-definition-id";
    private static final String TRANSFER_ID = "transfer-id";
    private static final String CUSTOMER_NAME = "customer-name";
    private static final String RESOURCE_NAME = "resource-name";
    private static final BigQueryTarget TEST_TARGET = new BigQueryTarget(TEST_PROJECT, TEST_DATASET, TEST_TABLE);
    private final Monitor monitor = mock();
    private final GcpConfiguration gcpConfiguration = mock();
    private final BigQuery bigQuery = mock();
    private final IamService iamService = mock();

    @Test
    void testCanProvisionTrue() {
        var bigQueryProvisioner = BigQueryProvisioner.Builder.newInstance(gcpConfiguration, null, null, monitor)
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
        var bigQueryProvisioner = BigQueryProvisioner.Builder.newInstance(gcpConfiguration, null, null, monitor)
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
        var bigQueryProvisioner = BigQueryProvisioner.Builder.newInstance(gcpConfiguration, null, null, monitor)
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
        var bigQueryProvisioner = BigQueryProvisioner.Builder.newInstance(gcpConfiguration, null, null, monitor)
                .build();

        assertThat(bigQueryProvisioner.canDeprovision(new ProvisionedResource() {
        })).isFalse();
    }

    @Test
    void provisionSuccessUsingAdc() {
        provisionSucceeds(null);
    }

    @Test
    void provisionSuccessUsingServiceAccount() {
        provisionSucceeds(TEST_SERVICE_ACCOUNT_NAME);
    }

    @Test
    void provisionFailsIfTableDoesntExist() {
        var bqFactory = new BigQueryFactoryTest();
        var bigQueryProvisioner = BigQueryProvisioner.Builder.newInstance(gcpConfiguration, bqFactory, null, monitor)
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

        var result = bigQueryProvisioner.provision(resourceDefinition, policy);
        assertThat(result).succeedsWithin(1, SECONDS)
                .extracting(StatusResult::failed)
                .isEqualTo(true);
    }

    private class BigQueryFactoryTest implements BigQueryFactory {
        public BigQuery createBigQuery(String serviceAccountName) throws IOException {
            return bigQuery;
        }
    }

    private void provisionSucceeds(String serviceAccountName) {
        var bqFactory = new BigQueryFactoryTest();
        var bigQueryProvisioner = BigQueryProvisioner.Builder.newInstance(gcpConfiguration, bqFactory, iamService, monitor)
                .build();

        var resourceDefinitionBuilder = BigQueryResourceDefinition.Builder.newInstance()
                .id(RESOURCE_ID)
                .transferProcessId(TRANSFER_ID)
                .property(BigQueryService.PROJECT, TEST_PROJECT)
                .property(BigQueryService.DATASET, TEST_DATASET)
                .property(BigQueryService.TABLE, TEST_TABLE)
                .property(BigQueryService.CUSTOMER_NAME, CUSTOMER_NAME);

        var now = fromMillis(System.currentTimeMillis());
        var expirationMillis = (now.getSeconds() + 3600) * 1000;
        var token = new GcpAccessToken(TEST_TOKEN, expirationMillis);
        var useAdc = false;
        GcpServiceAccount serviceAccount = null;

        if (serviceAccountName == null) {
            useAdc = true;
            serviceAccount = BigQueryProvisioner.ADC_SERVICE_ACCOUNT;
            serviceAccountName = serviceAccount.getName();
            when(iamService.createDefaultAccessToken()).thenReturn(token);
        } else {
            serviceAccount = new GcpServiceAccount(TEST_EMAIL, serviceAccountName, TEST_DESCRIPTION);
            when(iamService.getServiceAccount(serviceAccountName)).thenReturn(serviceAccount);
            resourceDefinitionBuilder.property(BigQueryService.SERVICE_ACCOUNT_NAME, serviceAccountName);
            when(iamService.createAccessToken(serviceAccount)).thenReturn(token);
        }

        var resourceDefinition = resourceDefinitionBuilder.build();
        var expectedResource = BigQueryProvisionedResource.Builder.newInstance()
                .properties(resourceDefinition.getProperties())
                .id(resourceDefinition.getId())
                .resourceDefinitionId(resourceDefinition.getId())
                .transferProcessId(resourceDefinition.getTransferProcessId())
                // TODO use proper constant.
                .resourceName(TEST_TABLE + "-table")
                .project(resourceDefinition.getProject())
                .dataset(resourceDefinition.getDataset())
                .table(TEST_TABLE)
                .serviceAccountName(serviceAccountName)
                .hasToken(true)
                .build();

        var policy = Policy.Builder.newInstance().build();
        var table = mock(Table.class);
        when(table.exists()).thenReturn(true);
        when(bigQuery.getTable(TEST_TARGET.getTableId())).thenReturn(table);

        var result = bigQueryProvisioner.provision(resourceDefinition, policy);

        if (!useAdc) {
            verify(iamService).getServiceAccount(serviceAccountName);
            verify(iamService).createAccessToken(serviceAccount);
        } else {
            verify(iamService).createDefaultAccessToken();
        }

        var content = assertThat(result).succeedsWithin(1, SECONDS)
                .extracting(responses -> {
                    try {
                        return result.get();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    } catch (ExecutionException e) {
                        throw new RuntimeException(e);
                    }
                })
                .extracting(StatusResult::getContent);

        content.extracting(ProvisionResponse::getResource)
                .extracting(BigQueryProvisionedResource.class::cast)
                .usingRecursiveComparison()
                .isEqualTo(expectedResource);

        content.extracting(ProvisionResponse::getSecretToken)
                .usingRecursiveComparison()
                .isEqualTo(token);
    }
}