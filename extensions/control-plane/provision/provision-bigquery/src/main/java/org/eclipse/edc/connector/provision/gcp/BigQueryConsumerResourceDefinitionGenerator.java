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

package org.eclipse.edc.connector.provision.gcp;

import org.eclipse.edc.connector.transfer.spi.provision.ConsumerResourceDefinitionGenerator;
import org.eclipse.edc.connector.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.connector.transfer.spi.types.TransferProcess;
import org.eclipse.edc.gcp.bigquery.service.BigQueryServiceSchema;
import org.eclipse.edc.policy.model.Policy;
import org.jetbrains.annotations.Nullable;

import static java.util.UUID.randomUUID;

public class BigQueryConsumerResourceDefinitionGenerator implements ConsumerResourceDefinitionGenerator {

    @Override
    public @Nullable
    ResourceDefinition generate(TransferProcess transferProcess, Policy policy) {
        var destination = transferProcess.getDataDestination();
        var id = randomUUID().toString();
        return BigQueryResourceDefinition.Builder.newInstance()
                .id(id)
                .properties(destination.getProperties())
                .build();
    }

    @Override
    public boolean canGenerate(TransferProcess transferProcess, Policy policy) {
        return BigQueryServiceSchema.BIGQUERY_DATA.equals(transferProcess.getDestinationType());
    }
}
