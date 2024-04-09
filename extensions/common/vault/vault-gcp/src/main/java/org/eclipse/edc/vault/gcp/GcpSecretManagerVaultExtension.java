/*
 *  Copyright (c) 2023 Google LLC
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Google LLC - Initial implementation
 *
 */

package org.eclipse.edc.vault.gcp;

import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Provider;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.eclipse.edc.util.string.StringUtils.isNullOrEmpty;

/**
 * ServiceExtension instantiating and registering Vault object.
 */
@Extension(value = GcpSecretManagerVaultExtension.NAME)
public class GcpSecretManagerVaultExtension implements ServiceExtension {

    public static final String NAME = "GCP Secret Manager";

    @Setting(value = "GCP Project for Vault", required = false)
    static final String VAULT_PROJECT = "edc.vault.gcp.project";

    @Setting(value = "JSON file with Service Account credentials", required = false)
    static final String VAULT_SACCOUNT_FILE = "edc.vault.gcp.saccount_file";

    @Setting(value = "GCP Region for Vault Secret replication", required = true)
    static final String VAULT_REGION = "edc.vault.gcp.region";

    @Inject
    GcpConfiguration gcpConfiguration;

    @Override
    public String name() {
        return NAME;
    }

    @Provider
    public Vault createVault(ServiceExtensionContext context) {
        var project = context.getSetting(VAULT_PROJECT, gcpConfiguration.projectId());
        var monitor = context.getMonitor();

        if (isNullOrEmpty(project)) {
            monitor.info("GCP Secret Manager vault extension: project loaded from default config " + project);
        } else {
            monitor.info("GCP Secret Manager vault extension: project loaded from settings " + project);
        }

        var saccountFile = context.getSetting(VAULT_SACCOUNT_FILE, gcpConfiguration.serviceAccountFile());

        // TODO support multi-region replica.
        var region = context.getConfig().getString(VAULT_REGION);
        monitor.info("GCP Secret Manager vault extension: region selected " + region);
        try {
            GcpSecretManagerVault vault;
            if (saccountFile == null) {
                monitor.info("Creating GCP Secret Manager vault extension with default access settings");
                vault = GcpSecretManagerVault.createWithDefaultSettings(monitor, project, region);
            } else {
                monitor.info("Creating GCP Secret Manager vault extension with Service Account credentials from file " + saccountFile);
                var credentialDataStream = Files.newInputStream(Paths.get(saccountFile));
                vault = GcpSecretManagerVault.createWithServiceAccountCredentials(monitor, project, region, credentialDataStream);
                credentialDataStream.close();
            }
            context.registerService(Vault.class, vault);
            return vault;
        } catch (IOException ioException) {
            throw new EdcException("Cannot create vault", ioException);
        }
    }
}
