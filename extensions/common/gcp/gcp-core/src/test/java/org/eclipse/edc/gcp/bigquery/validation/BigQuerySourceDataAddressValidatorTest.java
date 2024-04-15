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

public class BigQuerySourceDataAddressValidatorTest {
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_QUERY = "SELECT * from " + TEST_TABLE;
    private final BigQuerySourceDataAddressValidator validator = new BigQuerySourceDataAddressValidator();

    @Test
    void testSinkValidatorShouldValidateWellFormedAddresses() {
        var validSourceAddress = DataAddress.Builder.newInstance()
                .type(BigQueryServiceSchema.BIGQUERY_DATA)
                .property(BigQueryServiceSchema.QUERY, TEST_QUERY)
                .build();

        var validationResult = validator.validate(validSourceAddress);
        assertThat(validationResult.succeeded()).isTrue();
    }

    @Test
    void testSinkValidatorShouldNotValidateIncompleteAddresses() {
        var invalidSourceAddressNoQuery = DataAddress.Builder.newInstance()
                .type(BigQueryServiceSchema.BIGQUERY_DATA)
                .property(BigQueryServiceSchema.TABLE, TEST_TABLE)
                .build();

        assertThat(validator.validate(invalidSourceAddressNoQuery).failed()).isTrue();
    }
}
