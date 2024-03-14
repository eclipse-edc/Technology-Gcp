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

/**
 * Schema for BigQuery service.
 */
public interface BigQueryService {
    /** DataAddress type for BigQuery transfer. */
    String BIGQUERY_DATA = "BigQueryData";
    /** DataAddress type for BigQuery transfer. */
    String BIGQUERY_PROXY = "BigQueryProxy";
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
}
