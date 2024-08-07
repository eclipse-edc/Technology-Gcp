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

import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ProvisionManager;
import org.eclipse.edc.connector.controlplane.transfer.spi.provision.ResourceManifestGenerator;
import org.eclipse.edc.gcp.bigquery.BigQueryConfiguration;
import org.eclipse.edc.gcp.bigquery.service.BigQueryFactoryImpl;
import org.eclipse.edc.gcp.common.GcpAccessToken;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;

@Extension(value = BigQueryProvisionExtension.NAME)
public class BigQueryProvisionExtension implements ServiceExtension {
    public static final String NAME = "GCP BigQuery Provisioner";
    @Inject
    private ProvisionManager provisionManager;
    @Inject
    private ResourceManifestGenerator manifestGenerator;
    @Inject
    private GcpConfiguration gcpConfiguration;
    @Inject
    private IamService iamService;
    @Inject
    private TypeManager typeManager;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        var monitor = context.getMonitor();
        var bigQueryConfiguration = new BigQueryConfiguration(gcpConfiguration);
        var bqFactory = new BigQueryFactoryImpl(bigQueryConfiguration, iamService);
        var provisioner = new BigQueryProvisioner(gcpConfiguration, bqFactory, iamService, monitor);

        provisionManager.register(provisioner);
        manifestGenerator.registerGenerator(new BigQueryConsumerResourceDefinitionGenerator());
        typeManager.registerTypes(BigQueryProvisionedResource.class, BigQueryResourceDefinition.class, GcpAccessToken.class);
    }
}
