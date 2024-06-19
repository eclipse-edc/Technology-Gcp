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

package org.eclipse.edc.connector.dataplane.gcp.bigquery.params;

import org.eclipse.edc.gcp.bigquery.BigQueryTarget;
import org.eclipse.edc.spi.types.domain.DataAddress;

import java.util.Objects;

/**
 * Data class that represent the parameters of an BigQuery transfer request.
 */
public class  BigQueryRequestParams {
    private String project;
    private String dataset;
    private String table;
    private String destinationTable;
    private String query;
    private String serviceAccountName;
    private DataAddress sinkAddress;

    public String getProject() {
        return project;
    }

    public String getDataset() {
        return dataset;
    }

    public String getTable() {
        return table;
    }

    public String getDestinationTable() {
        return destinationTable;
    }

    public String getQuery() {
        return query;
    }

    public String getServiceAccountName() {
        return serviceAccountName;
    }

    public DataAddress getSinkAddress() {
        return sinkAddress;
    }

    public BigQueryTarget getTarget() {
        return new BigQueryTarget(project, dataset, table);
    }

    public static class Builder {
        private final BigQueryRequestParams params;

        public static BigQueryRequestParams.Builder newInstance() {
            return new BigQueryRequestParams.Builder();
        }

        private Builder() {
            params = new BigQueryRequestParams();
        }

        public BigQueryRequestParams.Builder project(String project) {
            params.project = project;
            return this;
        }

        public BigQueryRequestParams.Builder dataset(String dataset) {
            params.dataset = dataset;
            return this;
        }

        public BigQueryRequestParams.Builder table(String table) {
            params.table = table;
            return this;
        }

        public BigQueryRequestParams.Builder destinationTable(String destinationTable) {
            params.destinationTable = destinationTable;
            return this;
        }

        public BigQueryRequestParams.Builder query(String query) {
            params.query = query;
            return this;
        }

        public BigQueryRequestParams.Builder serviceAccountName(String serviceAccountName) {
            params.serviceAccountName = serviceAccountName;
            return this;
        }

        public BigQueryRequestParams.Builder sinkAddress(DataAddress sinkAddress) {
            params.sinkAddress = sinkAddress;
            return this;
        }

        public BigQueryRequestParams build() {
            Objects.requireNonNull(params.project, "project");
            return params;
        }
    }
}
