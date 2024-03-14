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

import org.eclipse.edc.connector.transfer.spi.types.DataRequest;
import org.eclipse.edc.connector.transfer.spi.types.TransferProcess;
import org.eclipse.edc.gcp.bigquery.service.BigQueryService;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.asset.Asset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

public class BigQueryConsumerResourceDefinitionGeneratorTest {
    private static final String TEST_PROJECT = "test-project";
    private static final String TEST_DATASET = "test-dataset";
    private static final String TEST_TABLE = "test-table";
    private BigQueryConsumerResourceDefinitionGenerator generator;

    @BeforeEach
    void setUp() {
        generator = new BigQueryConsumerResourceDefinitionGenerator();
    }

    @Test
    void generate() {
        var destination = DataAddress.Builder.newInstance().type(BigQueryService.BIGQUERY_DATA)
                .property(BigQueryService.PROJECT, TEST_PROJECT)
                .property(BigQueryService.DATASET, TEST_DATASET)
                .property(BigQueryService.TABLE, TEST_TABLE)
                .build();
        var asset = Asset.Builder.newInstance().build();
        var dataRequest = DataRequest.Builder.newInstance().dataDestination(destination).assetId(asset.getId()).build();
        var transferProcess = TransferProcess.Builder.newInstance().dataRequest(dataRequest).build();
        var policy = Policy.Builder.newInstance().build();

        var definition = generator.generate(transferProcess, policy);

        assertThat(definition).isInstanceOf(BigQueryResourceDefinition.class);
        var objectDef = (BigQueryResourceDefinition) definition;
        assertThat(objectDef.getProject()).isEqualTo(TEST_PROJECT);
        assertThat(objectDef.getDataset()).isEqualTo(TEST_DATASET);
        assertThat(objectDef.getId()).satisfies(UUID::fromString);
    }

    @Test
    void generate_noDataRequestAsParameter() {
        var policy = Policy.Builder.newInstance().build();
        assertThatExceptionOfType(NullPointerException.class).isThrownBy(() -> generator.generate(null, policy));
    }

    @Test
    void canGenerate() {
        var destination = DataAddress.Builder.newInstance().type(BigQueryService.BIGQUERY_DATA)
                .build();
        var asset = Asset.Builder.newInstance().build();
        var dataRequest = DataRequest.Builder.newInstance().dataDestination(destination).assetId(asset.getId()).build();
        var transferProcess = TransferProcess.Builder.newInstance().dataRequest(dataRequest).build();
        var policy = Policy.Builder.newInstance().build();

        var definition = generator.canGenerate(transferProcess, policy);
        assertThat(definition).isTrue();
    }

    @Test
    void canGenerate_isNotTypeBigQueryStream() {
        var destination = DataAddress.Builder.newInstance().type("NonBigQueryData")
                .build();
        var asset = Asset.Builder.newInstance().build();
        var dataRequest = DataRequest.Builder.newInstance().dataDestination(destination).assetId(asset.getId()).build();
        var transferProcess = TransferProcess.Builder.newInstance().dataRequest(dataRequest).build();
        var policy = Policy.Builder.newInstance().build();

        var definition = generator.canGenerate(transferProcess, policy);
        assertThat(definition).isFalse();
    }
}
