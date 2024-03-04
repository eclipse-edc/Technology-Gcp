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

import org.assertj.core.api.AbstractAssert;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource.Part;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

public class PartAssert extends AbstractAssert<PartAssert, Part> {
    public PartAssert(Part actual) {
        super(actual, PartAssert.class);
    }

    public PartAssert isEqualTo(Part expected) {
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
