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

package org.eclipse.edc.gcp.bigquery.validation;

import org.eclipse.edc.gcp.bigquery.service.BigQueryServiceSchema;
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
                .type(BigQueryServiceSchema.BIGQUERY_DATA)
                .property(BigQueryServiceSchema.DATASET, TEST_DATASET)
                .property(BigQueryServiceSchema.TABLE, TEST_TABLE)
                .build();

        var validationResult = validator.validate(validSinkAddress);
        assertThat(validationResult.succeeded()).isTrue();
    }

    @Test
    void testSinkValidatorShouldNotValidateWithoutTable() {
        var invalidSinkAddressNoTable = DataAddress.Builder.newInstance()
                .type(BigQueryServiceSchema.BIGQUERY_DATA)
                .property(BigQueryServiceSchema.DATASET, TEST_DATASET)
                .build();

        assertThat(validator.validate(invalidSinkAddressNoTable).failed()).isTrue();
    }

    @Test
    void testSinkValidatorShouldNotValidateWithoutDataset() {
        var invalidSinkAddressNoDataset = DataAddress.Builder.newInstance()
                .type(BigQueryServiceSchema.BIGQUERY_DATA)
                .property(BigQueryServiceSchema.TABLE, TEST_TABLE)
                .build();

        assertThat(validator.validate(invalidSinkAddressNoDataset).failed()).isTrue();
    }

    @Test
    void testSinkValidatorShouldNotValidateEmptyAddress() {
        var invalidSinkAddressNoInfo = DataAddress.Builder.newInstance()
                .type(BigQueryServiceSchema.BIGQUERY_DATA)
                .build();

        assertThat(validator.validate(invalidSinkAddressNoInfo).failed()).isTrue();
    }
}
