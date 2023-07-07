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

import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.when;

class GcpSecretManagerVaultExtensionTest {

    private final Monitor monitor = mock(Monitor.class);
    private final GcpSecretManagerVaultExtension extension = new GcpSecretManagerVaultExtension();

    private static final String TEST_REGION = "europe-west3";
    private static final String TEST_PROJECT = "project";
    private static final String TEST_FILE = "/file/path/account.json";

    @BeforeEach
    void resetMocks() {
        reset(monitor);
    }

    @Test
    void noSettings_shouldThrowException() {
        ServiceExtensionContext invalidContext = mock(ServiceExtensionContext.class);
        when(invalidContext.getMonitor()).thenReturn(monitor);

        EdcException exception = Assertions.assertThrows(EdcException.class, () -> extension.initialize(invalidContext));
        Assertions.assertEquals("setting 'edc.vault.gcp.region' is not provided", exception.getMessage());
    }

    @Test
    void onlyProjectSetting_shouldThrowException() {
        ServiceExtensionContext invalidContext = mock(ServiceExtensionContext.class);
        when(invalidContext.getMonitor()).thenReturn(monitor);
        when(invalidContext.getSetting(GcpSecretManagerVaultExtension.VAULT_PROJECT, null)).thenReturn(TEST_PROJECT);

        EdcException exception = Assertions.assertThrows(EdcException.class, () -> extension.initialize(invalidContext));
        Assertions.assertEquals("setting 'edc.vault.gcp.region' is not provided", exception.getMessage());
    }

    @Test
    void onlyRegionSetting_shouldNotThrowException() {
        ServiceExtensionContext validContext = mock(ServiceExtensionContext.class);
        when(validContext.getMonitor()).thenReturn(monitor);
        when(validContext.getSetting(GcpSecretManagerVaultExtension.VAULT_REGION, null)).thenReturn(TEST_REGION);

        try (MockedStatic<GcpSecretManagerVault> utilities = Mockito.mockStatic(GcpSecretManagerVault.class)) {
            utilities.when(() -> GcpSecretManagerVault.createWithDefaultSettings(monitor, TEST_PROJECT, TEST_REGION))
                    .thenReturn(new GcpSecretManagerVault(null, null, null, null));
            extension.initialize(validContext);
        }
    }

    @Test
    void mandatorySettings_shouldNotThrowException() {
        ServiceExtensionContext validContext = mock(ServiceExtensionContext.class);
        when(validContext.getMonitor()).thenReturn(monitor);
        when(validContext.getSetting(GcpSecretManagerVaultExtension.VAULT_PROJECT, null)).thenReturn(TEST_PROJECT);
        when(validContext.getSetting(GcpSecretManagerVaultExtension.VAULT_REGION, null)).thenReturn(TEST_REGION);

        try (MockedStatic<GcpSecretManagerVault> utilities = Mockito.mockStatic(GcpSecretManagerVault.class)) {
            utilities.when(() -> GcpSecretManagerVault.createWithDefaultSettings(monitor, TEST_PROJECT, TEST_REGION))
                    .thenReturn(new GcpSecretManagerVault(null, null, null, null));
            extension.initialize(validContext);
        }
    }

    @Test
    void mandatorySettingsWithServiceAccount_shouldNotThrowException() {
        ServiceExtensionContext validContext = mock(ServiceExtensionContext.class);
        when(validContext.getMonitor()).thenReturn(monitor);
        when(validContext.getSetting(GcpSecretManagerVaultExtension.VAULT_PROJECT, null)).thenReturn(TEST_PROJECT);
        when(validContext.getSetting(GcpSecretManagerVaultExtension.VAULT_REGION, null)).thenReturn(TEST_REGION);
        when(validContext.getSetting(GcpSecretManagerVaultExtension.VAULT_SACCOUNT_FILE, null)).thenReturn(TEST_FILE);

        try (MockedStatic<GcpSecretManagerVault> utilities = Mockito.mockStatic(GcpSecretManagerVault.class)) {
            utilities.when(() -> GcpSecretManagerVault.createWithServiceAccountCredentials(monitor, TEST_PROJECT, TEST_REGION, TEST_FILE))
                    .thenReturn(new GcpSecretManagerVault(null, null, null, null));
            extension.initialize(validContext);
        }
    }
}
