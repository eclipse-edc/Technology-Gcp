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
import org.eclipse.edc.gcp.bigquery.BigQueryConfiguration;
import org.eclipse.edc.gcp.bigquery.validation.BigQuerySourceDataAddressValidator;
import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;

import static org.eclipse.edc.gcp.bigquery.service.BigQueryServiceSchema.BIGQUERY_DATA;

/**
 * Instantiates the BigQuery data transfer source {@link BigQueryDataSource} for requests whose
 * source data type is BigQueryData. This factory is registered by the BigQuery Data Plane Extension.
 */
public class BigQueryDataSourceFactory implements DataSourceFactory {
    private final BigQueryConfiguration configuration;
    private final BigQueryRequestParamsProvider requestParamsProvider;
    private final Monitor monitor;
    private final ExecutorService executorService;
    private final BigQuerySourceDataAddressValidator sourceDataAddressValidator = new BigQuerySourceDataAddressValidator();
    private final IamService iamService;

    public BigQueryDataSourceFactory(BigQueryConfiguration configuration, Monitor monitor,
                                     BigQueryRequestParamsProvider requestParamsProvider,
                                     ExecutorService executorService,
                                     IamService iamService) {
        this.configuration = configuration;
        this.monitor = monitor;
        this.requestParamsProvider = requestParamsProvider;
        this.executorService = executorService;
        this.iamService = iamService;
    }

    @Override
    public @NotNull Result<Void> validateRequest(DataFlowStartMessage message) {
        // canHandle has been already invoked to have this factory selected.
        var sourceValidationresult = sourceDataAddressValidator.validate(message.getSourceDataAddress());
        if (sourceValidationresult.failed()) {
            return Result.failure(sourceValidationresult.getFailureDetail());
        }

        return Result.success();
    }

    @Override
    public String supportedType() {
        return BIGQUERY_DATA;
    }

    @Override
    public DataSource createSource(DataFlowStartMessage message) {
        var params = requestParamsProvider.provideSourceParams(message);
        var target = params.getTarget();

        GoogleCredentials credentials = null;
        if (configuration.restEndpoint() == null) {
            var serviceAccount = iamService.getServiceAccount(params.getServiceAccountName());
            credentials = iamService.getCredentials(serviceAccount, IamService.BQ_SCOPE);
        }

        return BigQueryDataSource.Builder.newInstance()
            .monitor(monitor)
            .executorService(executorService)
            .credentials(credentials)
            .target(target)
            .configuration(configuration)
            .requestId(message.getId())
            .params(params)
            .build();
    }
}
