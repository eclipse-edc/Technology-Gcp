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
 *       Google LCC - Initial implementation
 *
 */

package org.eclipse.edc.connector.provision.gcp;

import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.eclipse.edc.connector.transfer.spi.provision.ProvisionManager;
import org.eclipse.edc.connector.transfer.spi.provision.ResourceManifestGenerator;
import org.eclipse.edc.gcp.iam.IamServiceImpl;
import org.eclipse.edc.gcp.storage.StorageServiceImpl;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;


public class GcsProvisionExtension implements ServiceExtension {

    @Setting(value = "The GCP project ID", required = true)
    private static final String GCP_PROJECT_ID = "edc.gcp.project.id";

    @Inject
    private ProvisionManager provisionManager;

    @Inject
    private ResourceManifestGenerator manifestGenerator;

    @Override
    public String name() {
        return "GCP storage provisioner";
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        var monitor = context.getMonitor();

        var projectId = context.getConfig().getString(GCP_PROJECT_ID);
        var storageClient = createDefaultStorageClient(projectId);
        var storageService = new StorageServiceImpl(storageClient, monitor);
        var iamService = IamServiceImpl.Builder.newInstance(monitor, projectId).build();

        var provisioner = new GcsProvisioner(monitor, storageService, iamService);
        provisionManager.register(provisioner);

        manifestGenerator.registerGenerator(new GcsConsumerResourceDefinitionGenerator());
    }


    /**
     * Creates {@link Storage} for the specified project using application default credentials
     *
     * @param projectId The project that should be used for storage operations
     * @return {@link Storage}
     */
    private Storage createDefaultStorageClient(String projectId) {
        return StorageOptions.newBuilder().setProjectId(projectId).build().getService();
    }
}