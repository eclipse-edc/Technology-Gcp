/*
 *  Copyright (c) 2022 T-Systems International GmbH
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       T-Systems International GmbH
 *
 */

package org.eclipse.edc.connector.dataplane.gcp.storage;


import com.google.cloud.storage.StorageOptions;
import org.eclipse.edc.connector.dataplane.gcp.storage.validation.GcsSinkDataAddressValidator;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSink;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSinkFactory;
import org.eclipse.edc.gcp.common.GcpCredentials;
import org.eclipse.edc.gcp.common.GcpServiceAccountCredentials;
import org.eclipse.edc.gcp.storage.GcsStoreSchema;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowRequest;
import org.eclipse.edc.validator.spi.Validator;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;

public class GcsDataSinkFactory implements DataSinkFactory {

    private final Validator<DataAddress> validation = new GcsSinkDataAddressValidator();
    private final ExecutorService executorService;
    private final Monitor monitor;
    private final Vault vault;
    private final TypeManager typeManager;

    private final GcpCredentials gcpCredential;

    public GcsDataSinkFactory(ExecutorService executorService, Monitor monitor, Vault vault, TypeManager typeManager, GcpCredentials gcpCredential) {
        this.executorService = executorService;
        this.monitor = monitor;
        this.vault = vault;
        this.typeManager = typeManager;
        this.gcpCredential = gcpCredential;
    }

    @Override
    public boolean canHandle(DataFlowRequest request) {
        return GcsStoreSchema.TYPE.equals(request.getDestinationDataAddress().getType());
    }

    @Override
    public @NotNull Result<Void> validateRequest(DataFlowRequest request) {
        var destination = request.getDestinationDataAddress();
        return validation.validate(destination).toResult();
    }

    @Override
    public DataSink createSink(DataFlowRequest request) {
        var validationResult = validateRequest(request);
        if (validationResult.failed()) {
            throw new EdcException(String.join(", ", validationResult.getFailureMessages()));
        }

        var destination = request.getDestinationDataAddress();
        var tokenKeyName = destination.getKeyName();
        var serviceAccountKeyName = destination.getStringProperty(GcsStoreSchema.SERVICE_ACCOUNT_KEY_NAME);
        var serviceAccountValue = destination.getStringProperty(GcsStoreSchema.SERVICE_ACCOUNT_KEY_VALUE);
        var gcpServiceAccountCredentials = new GcpServiceAccountCredentials(tokenKeyName, serviceAccountKeyName, serviceAccountValue);
        var googleCredentials = gcpCredential.resolveGoogleCredentialsFromDataAddress(gcpServiceAccountCredentials);
        var storageClient = StorageOptions.newBuilder()
                .setCredentials(googleCredentials)
                .build().getService();
        return GcsDataSink.Builder.newInstance()
                .storageClient(storageClient)
                .bucketName(destination.getStringProperty(GcsStoreSchema.BUCKET_NAME))
                .blobName(destination.getStringProperty(GcsStoreSchema.BLOB_NAME))
                .requestId(request.getId())
                .executorService(executorService)
                .monitor(monitor)
                .build();
    }

}