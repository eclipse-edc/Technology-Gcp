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

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Table;
import org.eclipse.edc.gcp.bigquery.BigQueryTarget;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.types.TypeManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Random;

import static org.eclipse.edc.gcp.bigquery.service.Asserts.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

class BigQueryProvisionServiceImplTest {
    private static final String TEST_PROJECT = "test-project";
    private static final String TEST_DATASET = "test-dataset";
    private static final String TEST_TABLE = "test-table";
    private final BigQueryTarget target = new BigQueryTarget(TEST_PROJECT, TEST_DATASET, TEST_TABLE);
    private Random random;
    private BigQueryProvisionServiceImpl bqProvisionService;
    private Monitor monitor = mock();
    private GcpConfiguration gcpConfiguration = mock();
    private BigQuery bigQuery = mock();
    private Table table = mock();
    private TypeManager typeManager = mock();

    @BeforeEach
    void setUp() {
        reset(gcpConfiguration);
        reset(monitor);
        reset(bigQuery);
        reset(table);
        reset(typeManager);

        bqProvisionService = BigQueryProvisionServiceImpl.Builder.newInstance(gcpConfiguration, target, monitor)
            .bigQuery(bigQuery)
            .build();

        // New seed for each test.
        random = new Random();
    }

    @Test
    void testTableExistsTrue() {
        when(table.exists()).thenReturn(true);
        when(bigQuery.getTable(target.getTableId())).thenReturn(table);

        assertThat(bqProvisionService.tableExists()).isTrue();
    }

    @Test
    void testTableExistsFalse() {
        when(table.exists()).thenReturn(false);
        when(bigQuery.getTable(target.getTableId())).thenReturn(table);

        assertThat(bqProvisionService.tableExists()).isFalse();
    }
}
