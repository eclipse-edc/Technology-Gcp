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
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.system.configuration.ConfigFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

class GcpSecretManagerVaultExtensionTest {

    private final Monitor monitor = mock(Monitor.class);
    private final GcpSecretManagerVaultExtension extension = new GcpSecretManagerVaultExtension();

    private static final String TEST_REGION = "europe-west3";
    private static final String TEST_PROJECT = "project";
    private static final String TEST_FILE_PREFIX = "file";
    private static final String TEST_FILE_SUFFIX = ".json";

    @BeforeEach
    void resetMocks() {
        reset(monitor);
    }

    @Test
    void noSettings_shouldThrowException() {
        ServiceExtensionContext invalidContext = mock(ServiceExtensionContext.class);
        when(invalidContext.getMonitor()).thenReturn(monitor);
        when(invalidContext.getConfig()).thenReturn(ConfigFactory.empty());

        extension.gcpConfiguration = new GcpConfiguration(invalidContext);

        EdcException exception = assertThrows(EdcException.class, () -> extension.createVault(invalidContext));
        assertThat(exception.getMessage().equals("No setting found for key " + GcpSecretManagerVaultExtension.VAULT_REGION));
    }

    @Test
    void onlyProjectSetting_shouldThrowException() {
        ServiceExtensionContext invalidContext = mock(ServiceExtensionContext.class);
        when(invalidContext.getMonitor()).thenReturn(monitor);
        var settings = new HashMap<String, String>();
        settings.put(GcpSecretManagerVaultExtension.VAULT_PROJECT, TEST_PROJECT);
        when(invalidContext.getConfig()).thenReturn(ConfigFactory.fromMap(settings));

        extension.gcpConfiguration = new GcpConfiguration(invalidContext);

        EdcException exception = assertThrows(EdcException.class, () -> extension.createVault(invalidContext));
        assertThat(exception.getMessage().equals("No setting found for key " + GcpSecretManagerVaultExtension.VAULT_REGION));
    }

    @Test
    void onlyRegionSetting_shouldNotThrowException() {
        ServiceExtensionContext validContext = mock(ServiceExtensionContext.class);
        when(validContext.getMonitor()).thenReturn(monitor);
        var settings = new HashMap<String, String>();
        settings.put(GcpSecretManagerVaultExtension.VAULT_REGION, TEST_REGION);
        when(validContext.getConfig()).thenReturn(ConfigFactory.fromMap(settings));

        extension.gcpConfiguration = new GcpConfiguration(validContext);

        try (MockedStatic<GcpSecretManagerVault> utilities = Mockito.mockStatic(GcpSecretManagerVault.class)) {
            utilities.when(() -> GcpSecretManagerVault.createWithDefaultSettings(monitor, TEST_PROJECT, TEST_REGION))
                    .thenReturn(new GcpSecretManagerVault(null, null, null, null));
            extension.createVault(validContext);
        }
    }

    @Test
    void mandatorySettings_shouldNotThrowException() {
        ServiceExtensionContext validContext = mock(ServiceExtensionContext.class);
        when(validContext.getMonitor()).thenReturn(monitor);
        var settings = new HashMap<String, String>();
        settings.put(GcpSecretManagerVaultExtension.VAULT_PROJECT, TEST_PROJECT);
        settings.put(GcpSecretManagerVaultExtension.VAULT_REGION, TEST_REGION);
        when(validContext.getConfig()).thenReturn(ConfigFactory.fromMap(settings));

        extension.gcpConfiguration = new GcpConfiguration(validContext);

        try (MockedStatic<GcpSecretManagerVault> utilities = Mockito.mockStatic(GcpSecretManagerVault.class)) {
            utilities.when(() -> GcpSecretManagerVault.createWithDefaultSettings(monitor, TEST_PROJECT, TEST_REGION))
                    .thenReturn(new GcpSecretManagerVault(null, null, null, null));
            extension.createVault(validContext);
        }
    }

    @Test
    void mandatorySettingsWithServiceAccount_shouldNotThrowException() {
        try {
            var tempPath = Files.createTempFile(TEST_FILE_PREFIX, TEST_FILE_SUFFIX);
            var accountFilePath = tempPath.toString();
            Files.write(tempPath, ("test account data").getBytes());
            ServiceExtensionContext validContext = mock(ServiceExtensionContext.class);
            when(validContext.getMonitor()).thenReturn(monitor);
            var settings = new HashMap<String, String>();
            settings.put(GcpSecretManagerVaultExtension.VAULT_PROJECT, TEST_PROJECT);
            settings.put(GcpSecretManagerVaultExtension.VAULT_REGION, TEST_REGION);
            settings.put(GcpSecretManagerVaultExtension.VAULT_SACCOUNT_FILE, accountFilePath);
            when(validContext.getConfig()).thenReturn(ConfigFactory.fromMap(settings));

            extension.gcpConfiguration = new GcpConfiguration(validContext);

            try (MockedStatic<GcpSecretManagerVault> utilities = Mockito.mockStatic(GcpSecretManagerVault.class)) {
                utilities.when(() -> GcpSecretManagerVault.createWithServiceAccountCredentials(eq(monitor), eq(TEST_PROJECT), eq(TEST_REGION), Mockito.any(InputStream.class)))
                        .thenReturn(new GcpSecretManagerVault(null, null, null, null));
                extension.createVault(validContext);
            }
        } catch (IOException ioException) {
            fail("Cannot create temporary file for testing");
        }
    }
}
