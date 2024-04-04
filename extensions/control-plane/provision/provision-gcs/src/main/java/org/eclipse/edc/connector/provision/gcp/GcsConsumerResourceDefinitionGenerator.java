/*
 *  Copyright (c) 2022 Google LLC
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Google LLC - Initial implementation
 *       ZF Friedrichshafen AG - improvements (refactoring of generate method)
 *       SAP SE - refactoring
 *
 */

package org.eclipse.edc.connector.provision.gcp;

import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ConsumerResourceDefinitionGenerator;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcess;
import org.eclipse.edc.gcp.storage.GcsStoreSchema;
import org.eclipse.edc.policy.model.Policy;
import org.jetbrains.annotations.Nullable;

import static java.util.UUID.randomUUID;

public class GcsConsumerResourceDefinitionGenerator implements ConsumerResourceDefinitionGenerator {

    @Override
    public @Nullable
    ResourceDefinition generate(TransferProcess transferProcess, Policy policy) {
        var destination = transferProcess.getDataDestination();
        var id = randomUUID().toString();
        var location = destination.getStringProperty(GcsStoreSchema.LOCATION);
        var storageClass = destination.getStringProperty(GcsStoreSchema.STORAGE_CLASS);
        var bucketName = destination.getStringProperty(GcsStoreSchema.BUCKET_NAME);
        var serviceAccount = destination.getStringProperty(GcsStoreSchema.SERVICE_ACCOUNT_NAME);

        return GcsResourceDefinition.Builder.newInstance().id(id).location(location)
                .storageClass(storageClass).bucketName(bucketName).serviceAccountName(serviceAccount).build();
    }

    @Override
    public boolean canGenerate(TransferProcess transferProcess, Policy policy) {
        return GcsStoreSchema.TYPE.equals(transferProcess.getDestinationType());
    }
}
