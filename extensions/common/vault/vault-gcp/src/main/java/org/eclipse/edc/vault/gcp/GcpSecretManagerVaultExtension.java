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
 *       Google LCC - Initial implementation
 *
 */

package org.eclipse.edc.vault.gcp;

import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.runtime.metamodel.annotation.Provides;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.security.CertificateResolver;
import org.eclipse.edc.spi.security.PrivateKeyResolver;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.security.VaultCertificateResolver;
import org.eclipse.edc.spi.security.VaultPrivateKeyResolver;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.eclipse.edc.util.string.StringUtils.isNullOrEmpty;

/**
 * ServiceExtension instantiating and registering Vault object.
 */
@Provides({ Vault.class, PrivateKeyResolver.class, CertificateResolver.class })
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

    @Override
    public void initialize(ServiceExtensionContext context) {
        var project = context.getSetting(VAULT_PROJECT, gcpConfiguration.getProjectId());
        if (isNullOrEmpty(project)) {
            context.getMonitor().info("GCP Secret Manager vault extension: project loaded from default config " + project);
        } else {
            context.getMonitor().info("GCP Secret Manager vault extension: project loaded from settings " + project);
        }

        var saccountFile = context.getSetting(VAULT_SACCOUNT_FILE, gcpConfiguration.getServiceAccountFile());

        // TODO support multi-region replica.
        var region = context.getConfig().getString(VAULT_REGION);
        context.getMonitor().info("GCP Secret Manager vault extension: region selected " + region);
        try {
            GcpSecretManagerVault vault = null;
            if (saccountFile == null) {
                context.getMonitor().info("Creating GCP Secret Manager vault extension with default access settings");
                vault = GcpSecretManagerVault.createWithDefaultSettings(context.getMonitor(), project, region);
            } else {
                context.getMonitor().info("Creating GCP Secret Manager vault extension with Service Account credentials from file " + saccountFile);
                var credentialDataStream = Files.newInputStream(Paths.get(saccountFile));
                vault = GcpSecretManagerVault.createWithServiceAccountCredentials(context.getMonitor(), project, region, credentialDataStream);
                credentialDataStream.close();
            }
            context.registerService(Vault.class, vault);
            context.registerService(PrivateKeyResolver.class, new VaultPrivateKeyResolver(vault));
            context.registerService(CertificateResolver.class, new VaultCertificateResolver(vault));
        } catch (IOException ioException) {
            throw new EdcException("Cannot create vault", ioException);
        }
    }
}
