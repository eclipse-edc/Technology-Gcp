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

import org.eclipse.edc.connector.dataplane.gcp.bigquery.params.BigQueryRequestParamsProvider;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSink;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSinkFactory;
import org.eclipse.edc.gcp.bigquery.service.BigQuerySinkService;
import org.eclipse.edc.gcp.bigquery.service.BigQuerySinkServiceImpl;
import org.eclipse.edc.gcp.common.GcpAccessToken;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;

import static org.eclipse.edc.gcp.bigquery.service.BigQueryServiceSchema.BIGQUERY_DATA;

/**
 * Instantiates {@link BigQueryDataSink}
 */
public class BigQueryDataSinkFactory implements DataSinkFactory {
    private final GcpConfiguration gcpConfiguration;
    private final ExecutorService executorService;
    private final Monitor monitor;
    private final Vault vault;
    private final TypeManager typeManager;
    private final BigQueryRequestParamsProvider requestParamsProvider;
    private BigQuerySinkService bqSinkService;
    private IamService iamService;

    public BigQueryDataSinkFactory(
                               GcpConfiguration gcpConfiguration,
                               ExecutorService executorService,
                               Monitor monitor,
                               Vault vault,
                               TypeManager typeManager,
                               BigQueryRequestParamsProvider requestParamsProvider,
                               IamService iamService) {
        this.gcpConfiguration = gcpConfiguration;
        this.executorService = executorService;
        this.monitor = monitor;
        this.vault = vault;
        this.typeManager = typeManager;
        this.requestParamsProvider = requestParamsProvider;
        this.iamService = iamService;
    }

    @Override
    public boolean canHandle(DataFlowStartMessage message) {
        return BIGQUERY_DATA.equals(message.getDestinationDataAddress().getType());
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
    public DataSink createSink(DataFlowStartMessage message) {
        if (!canHandle(message)) {
            throw new GcpException("BigQuery Data Sink cannot create sink for request type " + message.getSourceDataAddress().getType());
        }

        monitor.info("BigQuery Data Sink Factory " + message.getId());
        var params = requestParamsProvider.provideSinkParams(message);
        var target = params.getTarget();

        if (bqSinkService != null) {
            monitor.info("BigQuery Data Sink Factory reusing existing connecting service");
            return BigQueryDataSink.Builder.newInstance()
                    .requestId(message.getId())
                    .executorService(executorService)
                    .monitor(monitor)
                    .sinkService(bqSinkService)
                    .build();
        }

        var bqSinkServiceBuilder = BigQuerySinkServiceImpl.Builder.newInstance(gcpConfiguration, target, monitor);
        var keyName = message.getDestinationDataAddress().getKeyName();
        var usingProvisionerAccessToken = false;
        if (keyName != null && !keyName.isEmpty()) {
            var credentialsContent = vault.resolveSecret(keyName);
            if (credentialsContent != null) {
                var gcsAccessToken = typeManager.readValue(credentialsContent,
                        GcpAccessToken.class);
                var credentials  = iamService.getCredentials(gcsAccessToken);
                bqSinkServiceBuilder.credentials(credentials);
                usingProvisionerAccessToken = true;
                monitor.info("BigQuery Data Sink using provisioner's access token");
            } else {
                monitor.warning("BigQuery Data Sink cannot fetch provisioner's access token: " + keyName);
            }
        }

        if (!usingProvisionerAccessToken && System.getProperty("EDC_GCP_BQRPC") == null) {
            var serviceAccount = iamService.getServiceAccount(params.getServiceAccountName());
            var credentials = iamService.getCredentials(serviceAccount, IamService.BQ_SCOPE);
            bqSinkServiceBuilder.credentials(credentials);
            monitor.info("BigQuery Data Sink using service account from data address: " + serviceAccount.getName());
        }

        var newBqSinkService = bqSinkServiceBuilder.build();
        monitor.info("BigQuery Data Sink Factory connecting service ready");

        return BigQueryDataSink.Builder.newInstance()
            .requestId(message.getId())
            .executorService(executorService)
            .monitor(monitor)
            .sinkService(newBqSinkService)
            .build();
    }

    public static class Builder {
        private BigQueryDataSinkFactory factory;

        private Builder(
                GcpConfiguration gcpConfiguration,
                ExecutorService executorService,
                Monitor monitor,
                Vault vault,
                TypeManager typeManager,
                BigQueryRequestParamsProvider requestParamsProvider, IamService iamService) {
            factory = new BigQueryDataSinkFactory(gcpConfiguration, executorService, monitor, vault, typeManager, requestParamsProvider, iamService);
        }

        public static Builder newInstance(
                GcpConfiguration gcpConfiguration,
                ExecutorService executorService,
                Monitor monitor,
                Vault vault,
                TypeManager typeManager,
                BigQueryRequestParamsProvider requestParamsProvider, IamService iamService) {
            return new Builder(gcpConfiguration, executorService, monitor, vault, typeManager, requestParamsProvider, iamService);
        }

        public Builder sinkService(BigQuerySinkService bqSinkService) {
            factory.bqSinkService = bqSinkService;
            return this;
        }

        public BigQueryDataSinkFactory build() {
            return factory;
        }
    }
}
