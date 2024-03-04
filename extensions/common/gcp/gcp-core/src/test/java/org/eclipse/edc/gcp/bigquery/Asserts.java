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

import org.assertj.core.api.Assertions;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource.Part;

public class Asserts extends Assertions {

    private Asserts() {}

    public static PartAssert assertThat(Part actual) {
        return new PartAssert(actual);
    }

    public static BigQueryPartAssert assertThat(BigQueryPart actual) {
        return new BigQueryPartAssert(actual);
    }
}
