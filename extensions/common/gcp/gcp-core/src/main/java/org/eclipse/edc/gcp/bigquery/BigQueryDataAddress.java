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

import org.eclipse.edc.gcp.bigquery.service.BigQueryServiceSchema;
import org.eclipse.edc.spi.types.domain.DataAddress;

import java.util.Optional;

import static java.util.Collections.emptyMap;

/**
 * This is a wrapper class for the {@link DataAddress} object, which has typed accessors for
 * properties specific to a BigQuery endpoint.
 */
public class BigQueryDataAddress extends DataAddress {
    private BigQueryDataAddress() {
        setType(BigQueryServiceSchema.BIGQUERY_DATA);
    }

    public String getProject() {
        return getStringProperty(BigQueryServiceSchema.PROJECT);
    }

    public String getDataset() {
        return getStringProperty(BigQueryServiceSchema.DATASET);
    }

    public String getTable() {
        return getStringProperty(BigQueryServiceSchema.TABLE);
    }

    public String getQuery() {
        return getStringProperty(BigQueryServiceSchema.QUERY);
    }

    public String getServiceAccountName() {
        return getStringProperty(BigQueryServiceSchema.SERVICE_ACCOUNT_NAME);
    }

    public static final class Builder extends DataAddress.Builder<BigQueryDataAddress, Builder> {
        private Builder() {
            super(new BigQueryDataAddress());
        }

        public static Builder newInstance() {
            return new Builder();
        }

        public Builder project(String project) {
            property(BigQueryServiceSchema.PROJECT, project);
            return this;
        }

        public Builder dataset(String dataset) {
            property(BigQueryServiceSchema.DATASET, dataset);
            return this;
        }

        public Builder table(String table) {
            property(BigQueryServiceSchema.TABLE, table);
            return this;
        }

        public Builder query(String query) {
            property(BigQueryServiceSchema.QUERY, query);
            return this;
        }

        public Builder serviceAccountName(String serviceAccountName) {
            property(BigQueryServiceSchema.SERVICE_ACCOUNT_NAME, serviceAccountName);
            return this;
        }

        @Override
        public BigQueryDataAddress build() {
            return address;
        }

        public Builder copyFrom(DataAddress other) {
            Optional.ofNullable(other)
                .map(DataAddress::getProperties)
                .orElse(emptyMap()).forEach(this::property);
            return this;
        }
    }
}
