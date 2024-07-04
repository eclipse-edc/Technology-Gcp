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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings;
import io.grpc.netty.shaded.io.grpc.netty.NettyChannelBuilder;
import org.eclipse.edc.connector.dataplane.gcp.bigquery.params.BigQueryRequestParamsProvider;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSink;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSinkFactory;
import org.eclipse.edc.gcp.bigquery.BigQueryConfiguration;
import org.eclipse.edc.gcp.bigquery.validation.BigQuerySinkDataAddressValidator;
import org.eclipse.edc.gcp.common.GcpAccessToken;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.concurrent.ExecutorService;

import static org.eclipse.edc.gcp.bigquery.service.BigQueryServiceSchema.BIGQUERY_DATA;

/**
 * Instantiates the BigQuery data transfer sink {@link BigQueryDataSink}, this factory is registered
 * by the BigQuery Data Plane Extension.
 */
public class BigQueryDataSinkFactory implements DataSinkFactory {
    private final BigQueryConfiguration configuration;
    private final ExecutorService executorService;
    private final Monitor monitor;
    private final Vault vault;
    private final TypeManager typeManager;
    private final BigQueryRequestParamsProvider requestParamsProvider;
    private final BigQuerySinkDataAddressValidator sinkDataAddressValidator = new BigQuerySinkDataAddressValidator();
    private IamService iamService;

    public BigQueryDataSinkFactory(
            BigQueryConfiguration configuration,
            ExecutorService executorService,
            Monitor monitor,
            Vault vault,
            TypeManager typeManager,
            BigQueryRequestParamsProvider requestParamsProvider,
            IamService iamService) {
        this.configuration = configuration;
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
        var sinkValidationresult = sinkDataAddressValidator.validate(message.getDestinationDataAddress());
        if (sinkValidationresult.failed()) {
            return Result.failure(sinkValidationresult.getFailureDetail());
        }

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

        var dataSinkBuilder = BigQueryDataSink.Builder.newInstance();
        var writeSettingsBuilder = BigQueryWriteSettings.newBuilder();

        var keyName = message.getDestinationDataAddress().getKeyName();
        var usingProvisionerAccessToken = false;
        if (keyName != null && !keyName.isEmpty()) {
            var credentialsContent = vault.resolveSecret(keyName);
            if (credentialsContent != null) {
                var gcsAccessToken = typeManager.readValue(credentialsContent,
                        GcpAccessToken.class);
                var credentials  = iamService.getCredentials(gcsAccessToken);

                dataSinkBuilder.credentials(credentials);

                usingProvisionerAccessToken = true;
                monitor.info("BigQuery Data Sink using provisioner's access token");
            } else {
                monitor.warning("BigQuery Data Sink cannot fetch provisioner's access token: " + keyName);
            }
        }

        var host = configuration.rpcEndpoint();
        if (!usingProvisionerAccessToken && host == null) {
            var serviceAccount = iamService.getServiceAccount(params.getServiceAccountName());
            var credentials = iamService.getCredentials(serviceAccount, IamService.BQ_SCOPE);
            dataSinkBuilder.credentials(credentials);
            writeSettingsBuilder.setCredentialsProvider(FixedCredentialsProvider.create(credentials));
            monitor.info("BigQuery Data Sink using service account from data address: " + serviceAccount.getName());
        } else if (!usingProvisionerAccessToken) {
            var index = host.indexOf("//");
            if (index != -1) {
                host = host.substring(index + 2);
            }

            writeSettingsBuilder.setEndpoint(host);
            writeSettingsBuilder.setTransportChannelProvider(
                    FixedTransportChannelProvider.create(
                        GrpcTransportChannel.create(
                            NettyChannelBuilder.forTarget(host).usePlaintext().build()
                        )
                    )
            );

            writeSettingsBuilder.setCredentialsProvider(NoCredentialsProvider.create());
        }

        try {
            var writeClient = BigQueryWriteClient.create(writeSettingsBuilder.build());

            return dataSinkBuilder
                .requestId(message.getId())
                .executorService(executorService)
                .monitor(monitor)
                .bigQueryTarget(target)
                .configuration(configuration)
                .objectMapper(typeManager.getMapper())
                .writeClient(writeClient)
                .build();
        } catch (IOException ioException) {
            throw new GcpException("BigQuery Data Sink cannot create write client", ioException);
        }
    }
}
