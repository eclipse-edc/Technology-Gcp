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

package org.eclipse.edc.connector.dataplane.gcp.bigquery.pipeline;

import org.eclipse.edc.connector.dataplane.gcp.bigquery.params.BigQueryRequestParams;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.gcp.bigquery.service.BigQuerySourceService;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.spi.monitor.Monitor;

import java.util.Objects;
import java.util.stream.Stream;

import static org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult.error;
import static org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult.success;

public class BigQueryDataSource implements DataSource {
    private BigQueryRequestParams params;
    private String requestId;
    private Monitor monitor;
    private BigQuerySourceService bqSourceService;

    @Override
    public void close() {
    }

    @Override
    public StreamResult<Stream<Part>> openPartStream() {
        try {
            var query = params.getQuery();
            return success(
                bqSourceService.runSourceQuery(query, params.getDestinationTable(), params.getSinkAddress()));
        } catch (InterruptedException interruptedException) {
            monitor.severe("BigQuery Source error while running query", interruptedException);
            return error("BigQuery Source error while running query");
        } catch (GcpException gcpException) {
            monitor.severe("BigQuery Source error while building the query", gcpException);
            return error("BigQuery Source error while building the query");
        }
    }

    private BigQueryDataSource() {
    }

    public static class Builder {
        private final BigQueryDataSource dataSource;

        public static Builder newInstance() {
            return new Builder();
        }

        private Builder() {
            dataSource = new BigQueryDataSource();
        }

        public Builder requestId(String requestId) {
            dataSource.requestId = requestId;
            return this;
        }

        public Builder monitor(Monitor monitor) {
            dataSource.monitor = monitor;
            return this;
        }

        public Builder params(BigQueryRequestParams params) {
            dataSource.params = params;
            return this;
        }

        public Builder sourceService(BigQuerySourceService bqSourceService) {
            dataSource.bqSourceService = bqSourceService;
            return this;
        }

        public BigQueryDataSource build() {
            Objects.requireNonNull(dataSource.requestId, "requestId");
            Objects.requireNonNull(dataSource.monitor, "monitor");
            Objects.requireNonNull(dataSource.bqSourceService, "bqSourceService");
            return dataSource;
        }
    }
}
