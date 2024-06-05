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

import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;

/**
 * Provides {@link DataFlowStartMessage} methods to prepare start message parameters from a transfer
 * request.
 */
public interface BigQueryRequestParamsProvider {

    /**
     * Provides BigQuery start message params for the BigQuery data source.
     */
    BigQueryRequestParams provideSourceParams(DataFlowStartMessage message);

    /**
     * Provides BigQuery start message params for the BigQuery data sink.
     */
    BigQueryRequestParams provideSinkParams(DataFlowStartMessage message);
}
