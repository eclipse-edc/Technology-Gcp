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
import org.eclipse.edc.connector.dataplane.gcp.bigquery.params.BigQueryRequestParamsProviderImpl;
import org.eclipse.edc.connector.dataplane.gcp.bigquery.pipeline.BigQueryDataSinkFactory;
import org.eclipse.edc.connector.dataplane.gcp.bigquery.pipeline.BigQueryDataSourceFactory;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataTransferExecutorServiceContainer;
import org.eclipse.edc.connector.dataplane.spi.pipeline.PipelineService;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Provides;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ExecutorInstrumentation;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;

import java.util.concurrent.Executors;

/**
 * Provides interfaces for reading and writing BigQuery data.
 */
@Provides(BigQueryRequestParamsProvider.class)
@Extension(value = DataPlaneBigQueryExtension.NAME)
public class DataPlaneBigQueryExtension implements ServiceExtension {
    public static final String NAME = "Data Plane BigQuery";
    @Setting(value = "BigQuery source thread pool size", required = false, type = "int")
    public static final String BIGQUERY_THREAD_POOL = "edc.gcp.bq.threads";
    private static final int DEFAULT_THREAD_POOL_SIZE = 5;
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

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        var monitor = context.getMonitor();
        var paramsProvider = new BigQueryRequestParamsProviderImpl();
        context.registerService(BigQueryRequestParamsProvider.class, paramsProvider);

        var threadPoolSize = context.getSetting(BIGQUERY_THREAD_POOL, DEFAULT_THREAD_POOL_SIZE);
        var executorService = executorInstrumentation.instrument(
                Executors.newFixedThreadPool(threadPoolSize), "BigQuery Source");

        var sourceFactory = new BigQueryDataSourceFactory(gcpConfiguration, monitor, paramsProvider, typeManager, executorService, iamService);
        pipelineService.registerFactory(sourceFactory);

        var sinkFactory = new BigQueryDataSinkFactory(gcpConfiguration, executorContainer.getExecutorService(), monitor, vault, typeManager, paramsProvider, iamService);
        pipelineService.registerFactory(sinkFactory);
    }
}
