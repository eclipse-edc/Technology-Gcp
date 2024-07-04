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

import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.storage.v1.TableName;

/**
 * Utility class for getting the TableId and the TableName from GCP project and BigQuery dataset
 * and table.
 */
public record BigQueryTarget(String project, String dataset, String table) {
    /**
     * Returns the TableId corresponding to project / dataset / table, used a parameter to e.g.
     * verify the existunce of a table in BigQuery API.
     */
    public TableId getTableId() {
        return TableId.of(project, dataset, table);
    }

    /**
     * Returns the TableId corresponding to a destination table used when executing queries.
     *
     * @param tableName the name of the table used as destination for queries.
     */
    public TableId getTableId(String tableName) {
        return TableId.of(project, dataset, tableName);
    }

    /**
     * Returns the TableName corresponding to project / dataset / table, used a parameter for the
     * BigQuery Storage API (by the sink).
     */
    public TableName getTableName() {
        return TableName.of(project, dataset, table);
    }
}
