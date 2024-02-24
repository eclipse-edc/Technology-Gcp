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


import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.eclipse.edc.connector.dataplane.gcp.storage.validation.GcsSinkDataAddressValidator;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSink;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSinkFactory;
import org.eclipse.edc.gcp.common.GcpAccessToken;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.gcp.storage.GcsStoreSchema;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.eclipse.edc.validator.spi.Validator;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.ExecutorService;

public class GcsDataSinkFactory implements DataSinkFactory {

    private final Validator<DataAddress> validation = new GcsSinkDataAddressValidator();
    private final ExecutorService executorService;
    private final Monitor monitor;
    private final Vault vault;
    private final TypeManager typeManager;


    public GcsDataSinkFactory(ExecutorService executorService, Monitor monitor, Vault vault, TypeManager typeManager) {
        this.executorService = executorService;
        this.monitor = monitor;
        this.vault = vault;
        this.typeManager = typeManager;
    }

    @Override
    public boolean canHandle(DataFlowStartMessage request) {
        return GcsStoreSchema.TYPE.equals(request.getDestinationDataAddress().getType());
    }

    @Override
    public @NotNull Result<Void> validateRequest(DataFlowStartMessage request) {
        var destination = request.getDestinationDataAddress();
        return validation.validate(destination).toResult();
    }

    @Override
    public DataSink createSink(DataFlowStartMessage request) {
        var validationResult = validateRequest(request);
        if (validationResult.failed()) {
            throw new EdcException(String.join(", ", validationResult.getFailureMessages()));
        }

        var destination = request.getDestinationDataAddress();

        var storageClient = createStorageClient(destination.getKeyName());

        return GcsDataSink.Builder.newInstance()
                .storageClient(storageClient)
                .bucketName(destination.getStringProperty(GcsStoreSchema.BUCKET_NAME))
                .blobName(destination.getStringProperty(GcsStoreSchema.BLOB_NAME))
                .requestId(request.getId())
                .executorService(executorService)
                .monitor(monitor)
                .build();
    }

    private Storage createStorageClient(String keyName) {
        GoogleCredentials googleCredentials;
        //Get credential from the token if it exists in the vault otherwise use the default credentials of the system.
        if (keyName != null && !keyName.isEmpty()) {
            var credentialsContent = vault.resolveSecret(keyName);
            var gcsAccessToken = typeManager.readValue(credentialsContent, GcpAccessToken.class);
            googleCredentials = GoogleCredentials.create(
                    new AccessToken(gcsAccessToken.getToken(),
                            new Date(gcsAccessToken.getExpiration()))
            );
        } else {
            try {
                googleCredentials = GoogleCredentials.getApplicationDefault();
            } catch (IOException e) {
                throw new GcpException("Error while getting the default credentials.", e);
            }
        }

        return StorageOptions.newBuilder()
                .setCredentials(googleCredentials)
                .build().getService();
    }
}
