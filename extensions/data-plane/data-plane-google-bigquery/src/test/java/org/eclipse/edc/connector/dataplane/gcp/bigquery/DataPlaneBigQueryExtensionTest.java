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

package org.eclipse.edc.connector.dataplane.gcp.bigquery;

import org.eclipse.edc.connector.controlplane.api.client.spi.transferprocess.TransferProcessApiClient;
import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.gcp.bigquery.service.BigQueryServiceSchema;
import org.eclipse.edc.junit.extensions.EdcExtension;
import org.eclipse.edc.keys.spi.LocalPublicKeyService;
import org.eclipse.edc.keys.spi.PrivateKeyResolver;
import org.eclipse.edc.spi.query.CriterionOperatorRegistry;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.eclipse.edc.validator.spi.DataAddressValidatorRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

@ExtendWith(EdcExtension.class)
class DataPlaneBigQueryExtensionTest {
    private static final String TEST_PROJECT = "test-project";
    private static final String TEST_QUERY = "select * from table;";
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_DATASET = "test-dataset";

    @BeforeEach
    void setup(EdcExtension extension) {
        extension.registerServiceMock(TransferProcessApiClient.class, mock(TransferProcessApiClient.class));
        extension.registerServiceMock(TypeManager.class, mock(TypeManager.class));
        extension.registerServiceMock(CriterionOperatorRegistry.class, mock(CriterionOperatorRegistry.class));
        extension.registerServiceMock(DataAddressValidatorRegistry.class, mock(DataAddressValidatorRegistry.class));
        extension.registerServiceMock(LocalPublicKeyService.class, mock(LocalPublicKeyService.class));
        extension.registerServiceMock(PrivateKeyResolver.class, mock(PrivateKeyResolver.class));
    }

    @Test
    void pipelineServiceValidateSucceeds(PipelineService pipelineService) {
        var message = DataFlowStartMessage.Builder.newInstance()
                .processId(UUID.randomUUID().toString())
                .sourceDataAddress(getSourceDataAddress())
                .destinationDataAddress(getDestinationDataAddress())
                .build();

        assertThat(pipelineService.validate(message).succeeded()).isTrue();
    }

    private static DataAddress getSourceDataAddress() {
        return DataAddress.Builder.newInstance()
              .type(BigQueryServiceSchema.BIGQUERY_DATA)
              .property(BigQueryServiceSchema.QUERY, TEST_QUERY)
              .build();
    }

    private static DataAddress getDestinationDataAddress() {
        return DataAddress.Builder.newInstance()
            .type(BigQueryServiceSchema.BIGQUERY_DATA)
            .property(BigQueryServiceSchema.DATASET, TEST_DATASET)
            .property(BigQueryServiceSchema.TABLE, TEST_TABLE)
            .build();
    }
}
