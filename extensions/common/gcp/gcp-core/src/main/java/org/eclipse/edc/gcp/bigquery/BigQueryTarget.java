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

public record BigQueryTarget(String project, String dataset, String table) {
    public TableId getTableId() {
        return TableId.of(project, dataset, table);
    }

    public TableName getTableName() {
        return TableName.of(project, dataset, table);
    }
}
