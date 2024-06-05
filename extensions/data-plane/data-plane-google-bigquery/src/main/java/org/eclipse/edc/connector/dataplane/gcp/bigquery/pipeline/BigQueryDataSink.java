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

import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.StreamResult;
import org.eclipse.edc.connector.dataplane.util.sink.ParallelSink;
import org.eclipse.edc.gcp.bigquery.service.BigQuerySinkService;

import java.util.List;

/**
 * Writes data in a streaming fashion to an HTTP endpoint.
 */
public class BigQueryDataSink extends ParallelSink {
    private BigQuerySinkService bqSinkService;

    @Override
    protected StreamResult<Object> transferParts(List<DataSource.Part> parts) {
        try {
            bqSinkService.runSinkQuery(parts);
        } catch (Exception e) {
            monitor.severe("BigQuery Sink error writing data to the table", e);
            return StreamResult.error("BigQuery Sink error writing data to the table");
        }

        return StreamResult.success();
    }

    private BigQueryDataSink() {
    }

    public static class Builder extends ParallelSink.Builder<Builder, BigQueryDataSink> {

        public static Builder newInstance() {
            return new Builder();
        }

        public Builder sinkService(BigQuerySinkService bqSinkService) {
            sink.bqSinkService = bqSinkService;
            return this;
        }

        private Builder() {
            super(new BigQueryDataSink());
        }

        @Override
        protected void validate() {
        }
    }
}
