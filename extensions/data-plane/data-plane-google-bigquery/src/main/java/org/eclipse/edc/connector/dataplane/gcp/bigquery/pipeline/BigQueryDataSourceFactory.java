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

import com.google.auth.oauth2.GoogleCredentials;
import org.eclipse.edc.connector.dataplane.gcp.bigquery.params.BigQueryRequestParamsProvider;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSourceFactory;
import org.eclipse.edc.gcp.bigquery.service.BigQueryServiceSchema;
import org.eclipse.edc.gcp.bigquery.service.BigQuerySourceService;
import org.eclipse.edc.gcp.bigquery.service.BigQuerySourceServiceImpl;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;

import static org.eclipse.edc.gcp.bigquery.service.BigQueryServiceSchema.BIGQUERY_DATA;

/**
 * Instantiates {@link BigQueryDataSource}s for requests whose source data type is BigQueryRequest.
 */
public class BigQueryDataSourceFactory implements DataSourceFactory {
    private final GcpConfiguration gcpConfiguration;
    private final BigQueryRequestParamsProvider requestParamsProvider;
    private final Monitor monitor;
    private final TypeManager typeManager;
    private final ExecutorService executorService;
    private BigQuerySourceService bqSourceService;
    private IamService iamService;

    public BigQueryDataSourceFactory(GcpConfiguration gcpConfiguration, Monitor monitor,
                                     BigQueryRequestParamsProvider requestParamsProvider,
                                     TypeManager typeManager, ExecutorService executorService,
                                     IamService iamService) {
        this.gcpConfiguration = gcpConfiguration;
        this.monitor = monitor;
        this.requestParamsProvider = requestParamsProvider;
        this.typeManager = typeManager;
        this.executorService = executorService;
        this.iamService = iamService;
    }

    @Override
    public boolean canHandle(DataFlowStartMessage message) {
        return BigQueryServiceSchema.BIGQUERY_DATA.equals(message.getSourceDataAddress().getType());
    }

    @Override
    public @NotNull Result<Void> validateRequest(DataFlowStartMessage message) {
        // canHandle has been already invoked to have this factory selected.
        // BigQuerySinkDataAddressValidator has already checked message.getDestinationDataAddress().
        // BigQuerySourceDataAddressValidator has already checked message.getSourceDataAddress().

        return Result.success();
    }

    @Override
    public String supportedType() {
        return BIGQUERY_DATA;
    }

    @Override
    public DataSource createSource(DataFlowStartMessage message) {
        if (!canHandle(message)) {
            throw new GcpException("BigQuery Data Source cannot create source for request type " + message.getSourceDataAddress().getType());
        }

        monitor.info("BigQuery Data Source Factory " + message.getId());
        var params = requestParamsProvider.provideSourceParams(message);
        var target = params.getTarget();

        if (bqSourceService != null) {
            monitor.info("BigQuery Data Source Factory reusing existing connecting service");
            return BigQueryDataSource.Builder.newInstance()
                .monitor(monitor)
                .requestId(message.getId())
                .params(params)
                .sourceService(bqSourceService)
                .build();
        }

        GoogleCredentials credentials = null;
        if (System.getProperty("EDC_GCP_BQREST") == null) {
            var serviceAccount = iamService.getServiceAccount(params.getServiceAccountName());
            credentials = iamService.getCredentials(serviceAccount, IamService.BQ_SCOPE);
        }
        var bqSourceServiceBuilder = BigQuerySourceServiceImpl.Builder.newInstance(gcpConfiguration,
                target, monitor, executorService, credentials);
        var bqSourceService = bqSourceServiceBuilder.build();
        monitor.info("BigQuery Data Source Factory connecting service ready");

        return BigQueryDataSource.Builder.newInstance()
            .monitor(monitor)
            .requestId(message.getId())
            .params(params)
            .sourceService(bqSourceService)
            .build();
    }

    public static class Builder {
        private BigQueryDataSourceFactory factory;

        private Builder(
                GcpConfiguration gcpConfiguration,
                Monitor monitor,
                BigQueryRequestParamsProvider requestParamsProvider,
                TypeManager typeManager, ExecutorService executorService, IamService iamService) {
            factory = new BigQueryDataSourceFactory(gcpConfiguration, monitor, requestParamsProvider, typeManager, executorService, iamService);
        }

        public static Builder newInstance(
                GcpConfiguration gcpConfiguration,
                Monitor monitor,
                BigQueryRequestParamsProvider requestParamsProvider,
                TypeManager typeManager, ExecutorService executorService, IamService iamService) {
            return new Builder(gcpConfiguration, monitor, requestParamsProvider, typeManager, executorService, iamService);
        }

        public Builder sourceService(BigQuerySourceService bqSourceService) {
            factory.bqSourceService = bqSourceService;
            return this;
        }

        public BigQueryDataSourceFactory build() {
            return factory;
        }
    }
}
