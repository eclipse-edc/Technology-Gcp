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
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource.Part;
import org.eclipse.edc.spi.types.domain.DataAddress;

import java.util.List;
import java.util.stream.Stream;

/**
 * Interface for BigQuery service.
 */
public interface BigQueryService {
    /** DataAddress type for BigQuery transfer. */
    String BIGQUERY_DATA = "BigQueryData";
    /** Project including dataset and table for BigQuery DataAddress, overrides the project defined in connector config. */
    String PROJECT = "project";
    /** Dataset including the table for BigQuery DataAddress. */
    String DATASET = "dataset";
    /** Table name for the BigQuery DataAddress. */
    String TABLE = "table";
    /** Query used to extract data by the BigQuery source. */
    String QUERY = "query";
    /** Service account used to access BigQuery service, overrides the service account defined in connector config. */
    String SERVICE_ACCOUNT_NAME = "service_account_name";
    /** Customer name used to label the query in the BigQuery source. */
    String CUSTOMER_NAME = "customer_name";

    /**
     * Executes the given query and returns the result as stream.
     *
     * @param query the query string to be executed.
     * @param sinkAddress the target address for the sink.
     *
     * @return the stream of DataSource.Part objects.
     */
    Stream<DataSource.Part> runSourceQuery(String query, DataAddress sinkAddress) throws InterruptedException;

    /**
     * Executes the given query to insert values into the target table.
     *
     * @param parts list of rows to be inserted.
     */
    void runSinkQuery(List<Part> parts);

    /**
     * Checks whether the table defined exists.
     *
     * @param target includes the name of the table, within a dataset and a project.
     * @return true if the table specified exists.
     */
    boolean tableExists(BigQueryTarget target);
}
