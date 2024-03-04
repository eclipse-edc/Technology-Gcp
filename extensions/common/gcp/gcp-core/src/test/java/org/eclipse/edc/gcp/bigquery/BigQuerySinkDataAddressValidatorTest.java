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

package org.eclipse.edc.gcp.bigquery;

import org.eclipse.edc.spi.types.domain.DataAddress;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

public class BigQuerySinkDataAddressValidatorTest {
    private static final String TEST_DATASET = "test-dataset";
    private static final String TEST_TABLE = "test-table";
    private final BigQuerySinkDataAddressValidator validator = new BigQuerySinkDataAddressValidator();

    @Test
    void testSinkValidatorShouldValidateWellFormedAddresses() {
        var validSinkAddress = DataAddress.Builder.newInstance()
                .type(BigQueryService.BIGQUERY_DATA)
                .property(BigQueryService.DATASET, TEST_DATASET)
                .property(BigQueryService.TABLE, TEST_TABLE)
                .build();

        var validationResult = validator.validate(validSinkAddress);
        assertThat(validationResult.succeeded()).isTrue();
    }

    @Test
    void testSinkValidatorShouldNotValidateIncompleteAddresses() {
        var invalidSinkAddressNoTable = DataAddress.Builder.newInstance()
                .type(BigQueryService.BIGQUERY_DATA)
                .property(BigQueryService.DATASET, TEST_DATASET)
                .build();

        assertThat(validator.validate(invalidSinkAddressNoTable).failed()).isTrue();

        var invalidSinkAddressNoDataset = DataAddress.Builder.newInstance()
                .type(BigQueryService.BIGQUERY_DATA)
                .property(BigQueryService.TABLE, TEST_TABLE)
                .build();

        assertThat(validator.validate(invalidSinkAddressNoDataset).failed()).isTrue();

        var invalidSinkAddressNoInfo = DataAddress.Builder.newInstance()
                .type(BigQueryService.BIGQUERY_DATA)
                .build();

        assertThat(validator.validate(invalidSinkAddressNoInfo).failed()).isTrue();
    }
}
