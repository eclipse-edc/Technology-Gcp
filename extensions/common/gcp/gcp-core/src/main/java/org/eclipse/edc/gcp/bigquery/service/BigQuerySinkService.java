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

import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource.Part;

import java.util.List;

/**
 * Interface for BigQuery service.
 */
public interface BigQuerySinkService {
    /**
     * Executes the given query to insert values into the target table.
     *
     * @param parts list of rows to be inserted.
     */
    void runSinkQuery(List<Part> parts);
}
