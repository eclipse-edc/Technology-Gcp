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

import org.eclipse.edc.gcp.bigquery.BigQueryDataAddress;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;

/**
 * Provides {@link DataFlowStartMessage} methods to prepare start message parameters from a
 * BigQuery transfer request.
 */
public class BigQueryRequestParamsProvider {
    /**
     * Provides BigQuery start message params for the BigQuery data source.
     */
    public BigQueryRequestParams provideSourceParams(DataFlowStartMessage message) {
        var bqAddress = BigQueryDataAddress.Builder.newInstance()
                .copyFrom(message.getSourceDataAddress())
                .build();
        return getParamsBuilder(message.getSourceDataAddress())
                .sinkAddress(message.getDestinationDataAddress())
                .destinationTable(bqAddress.getDestinationTable())
                .build();
    }

    /**
     * Provides BigQuery start message params for the BigQuery data sink.
     */
    public BigQueryRequestParams provideSinkParams(DataFlowStartMessage message) {
        return getParamsBuilder(message.getDestinationDataAddress()).build();
    }

    private BigQueryRequestParams.Builder getParamsBuilder(DataAddress address) {
        var bqAddress = BigQueryDataAddress.Builder.newInstance()
                .copyFrom(address)
                .build();
        return BigQueryRequestParams.Builder.newInstance()
                .project(bqAddress.getProject())
                .dataset(bqAddress.getDataset())
                .table(bqAddress.getTable())
                .query(bqAddress.getQuery())
                .serviceAccountName(bqAddress.getServiceAccountName());
    }
}
