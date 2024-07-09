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

package org.eclipse.edc.connector.dataplane.gcp.bigquery;

import org.eclipse.edc.connector.dataplane.gcp.bigquery.params.BigQueryRequestParamsProvider;
import org.eclipse.edc.connector.dataplane.gcp.bigquery.pipeline.BigQueryDataSinkFactory;
import org.eclipse.edc.connector.dataplane.gcp.bigquery.pipeline.BigQueryDataSourceFactory;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataTransferExecutorServiceContainer;
import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.gcp.bigquery.BigQueryConfiguration;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Provider;
import org.eclipse.edc.runtime.metamodel.annotation.Provides;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ExecutorInstrumentation;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.util.configuration.ConfigurationFunctions;

import java.util.concurrent.Executors;

/**
 * Registers source and sink factories for BigQuery data transfers.
 */
@Provides(BigQueryRequestParamsProvider.class)
@Extension(value = DataPlaneBigQueryExtension.NAME)
public class DataPlaneBigQueryExtension implements ServiceExtension {
    public static final String NAME = "Data Plane BigQuery";
    static final int DEFAULT_THREAD_POOL_SIZE = 5;
    @Setting(value = "BigQuery source thread pool size", required = false, type = "int", defaultValue = "" + DEFAULT_THREAD_POOL_SIZE)
    public static final String BIGQUERY_THREAD_POOL = "edc.gcp.bq.threads";
    @Setting(value = "BigQuery API REST host, for testing purpose", required = false, type = "string")
    public static final String BIGQUERY_REST_ENDPOINT = "edc.gcp.bq.rest";
    @Setting(value = "BigQuery Storage API RPC host, for testing purpose", required = false, type = "string")
    public static final String BIGQUERY_RPC_ENDPOINT = "edc.gcp.bq.rpc";
    @Inject
    private PipelineService pipelineService;
    @Inject
    private GcpConfiguration gcpConfiguration;
    @Inject
    private Vault vault;
    @Inject
    private TypeManager typeManager;
    @Inject
    private DataTransferExecutorServiceContainer executorContainer;
    @Inject
    private ExecutorInstrumentation executorInstrumentation;
    @Inject
    private IamService iamService;
    private BigQueryConfiguration bigQueryConfiguration;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        var monitor = context.getMonitor();
        var paramsProvider = new BigQueryRequestParamsProvider();
        context.registerService(BigQueryRequestParamsProvider.class, paramsProvider);
        bigQueryConfiguration = getBigQueryConfiguration(gcpConfiguration, context);

        var executorService = executorInstrumentation.instrument(
                Executors.newFixedThreadPool(bigQueryConfiguration.threadPoolSize()), "BigQuery Source");

        var sourceFactory = new BigQueryDataSourceFactory(bigQueryConfiguration, monitor, paramsProvider, typeManager, executorService, iamService);
        pipelineService.registerFactory(sourceFactory);

        var sinkFactory = new BigQueryDataSinkFactory(bigQueryConfiguration, executorContainer.getExecutorService(), monitor, vault, typeManager, paramsProvider, iamService);
        pipelineService.registerFactory(sinkFactory);
    }

    @Provider
    public BigQueryConfiguration getBigQueryConfiguration() {
        return bigQueryConfiguration;
    }

    private BigQueryConfiguration getBigQueryConfiguration(GcpConfiguration gcpConfiguration, ServiceExtensionContext context) {
        var restEndpoint = context.getSetting(BIGQUERY_REST_ENDPOINT, null);
        var rpcEndpoint = context.getSetting(BIGQUERY_RPC_ENDPOINT, null);
        var threadPoolSize = context.getSetting(BIGQUERY_THREAD_POOL, DataPlaneBigQueryExtension.DEFAULT_THREAD_POOL_SIZE);

        if (restEndpoint == null) {
            ConfigurationFunctions.propOrEnv(BIGQUERY_REST_ENDPOINT, null);
        }

        if (rpcEndpoint == null) {
            ConfigurationFunctions.propOrEnv(BIGQUERY_RPC_ENDPOINT, null);
        }

        return new BigQueryConfiguration(gcpConfiguration, restEndpoint, rpcEndpoint, threadPoolSize);
    }
}
