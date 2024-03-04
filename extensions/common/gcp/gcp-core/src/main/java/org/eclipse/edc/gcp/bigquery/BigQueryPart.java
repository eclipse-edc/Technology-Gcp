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

import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;

import java.io.InputStream;

public class BigQueryPart implements DataSource.Part {
    private final String name;
    private final InputStream inputStream;

    public BigQueryPart(String name, InputStream inputStream) {
        this.name = name;
        this.inputStream = inputStream;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public InputStream openStream() {
        return inputStream;
    }

    @Override
    public long size() {
        return SIZE_UNKNOWN;
    }
}
