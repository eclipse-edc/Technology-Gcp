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

import org.assertj.core.api.Assertions;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource.Part;
import org.eclipse.edc.gcp.bigquery.BigQueryPart;

/**
 * Provides overloading for assertThat, to use specific assertions.
 */
class Asserts extends Assertions {

    private Asserts() {}

    static PartAssert assertThat(Part actual) {
        return new PartAssert(actual);
    }

    static BigQueryPartAssert assertThat(BigQueryPart actual) {
        return new BigQueryPartAssert(actual);
    }
}
