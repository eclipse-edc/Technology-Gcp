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

import org.assertj.core.api.AbstractAssert;
import org.eclipse.edc.gcp.bigquery.BigQueryPart;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Provides isEqualTo method to compare directly two BigQueryPart objects.
 */
class BigQueryPartAssert extends AbstractAssert<BigQueryPartAssert, BigQueryPart> {
    BigQueryPartAssert(BigQueryPart actual) {
        super(actual, BigQueryPartAssert.class);
    }

    BigQueryPartAssert isEqualTo(BigQueryPart expected) {
        isNotNull();
        assertThat(actual.name()).isEqualTo(expected.name());

        try {
            var actualPayload = new String(actual.openStream().readAllBytes(),
                    StandardCharsets.UTF_8);
            var expectedPayload = new String(expected.openStream().readAllBytes(),
                    StandardCharsets.UTF_8);

            assertThat(actualPayload).isEqualTo(expectedPayload);
        } catch (IOException ioException) {
            failWithMessage(ioException.getMessage());
        }

        return this;
    }
}
