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

import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.spi.types.domain.DataAddress;

import java.util.stream.Stream;

/**
 * Interface for BigQuery service.
 */
public interface BigQuerySourceService {
    /**
     * Executes the given query and returns the result as stream.
     *
     * @param query the query string to be executed.
     * @param sinkAddress the target address for the sink.
     *
     * @return the stream of DataSource.Part objects.
     */
    Stream<DataSource.Part> runSourceQuery(String query, DataAddress sinkAddress) throws InterruptedException;
}
