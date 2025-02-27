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
import org.eclipse.edc.connector.dataplane.gcp.storage.validation.GcsSourceDataAddressValidator;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSource;
import org.eclipse.edc.connector.dataplane.spi.pipeline.DataSourceFactory;
import org.eclipse.edc.gcp.storage.GcsStoreSchema;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.result.Result;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.eclipse.edc.validator.spi.Validator;
import org.jetbrains.annotations.NotNull;

public class GcsDataSourceFactory implements DataSourceFactory {

    private final Validator<DataAddress> validation = new GcsSourceDataAddressValidator();
    private final Monitor monitor;

    public GcsDataSourceFactory(Monitor monitor) {
        this.monitor = monitor;
    }

    @Override
    public String supportedType() {
        return GcsStoreSchema.TYPE;
    }

    @Override
    public @NotNull Result<Void> validateRequest(DataFlowStartMessage request) {
        var source = request.getSourceDataAddress();
        return validation.validate(source).toResult();
    }

    @Override
    public DataSource createSource(DataFlowStartMessage request) {
        var validationResult = validateRequest(request);
        if (validationResult.failed()) {
            throw new EdcException(String.join(", ", validationResult.getFailureMessages()));
        }
        var storageClient = StorageOptions.newBuilder()
                .build().getService();

        var source = request.getSourceDataAddress();

        return GcsDataSource.Builder.newInstance()
                .storageClient(storageClient)
                .bucketName(source.getStringProperty(GcsStoreSchema.BUCKET_NAME))
                .blobName(source.getStringProperty(GcsStoreSchema.BLOB_NAME))
                .monitor(monitor)
                .build();

    }

}
