/*
 *  Copyright (c) 2023 T-Systems International GmbH
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

package org.eclipse.edc.gcp.common;

import org.eclipse.edc.junit.extensions.DependencyInjectionExtension;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.spi.types.TypeManager;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Base64;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(DependencyInjectionExtension.class)
public class GcpCredentialsTest {

    private GcpCredentials gcpCredential;

    private final String accessTokenKeyName = "access_token_key_name_test";

    private final String invalidAccessTokenKeyName = "invalid_access_token_key_name_test";

    private final String serviceAccountKeyName = "service_account_key_name_test";

    private final String invalidServiceAccountKeyName = "invalid_service_account_key_name_test";


    private String serviceAccountFileInB64;

    @BeforeEach
    public void setUp(ServiceExtensionContext context) {
        var vault = mock(Vault.class);
        context.registerService(Vault.class, vault);
        var tokenValue = UUID.randomUUID();
        var privateKey = """
                -----BEGIN PRIVATE KEY-----
                MIIEuwIBADANBgkqhkiG9w0BAQEFAASCBKUwggShAgEAAoIBAQC6zzVgbCoFq0WS
                hZBsaW70ntwOmSVfufvaNtVahXU1kLqJ+h5chwXENyQ5A4md941KS/x+lpK1Z/Nv
                Wwk/G7Uf5N5GI006JShKVsXeTl/CbAh8cLDHgG35ZXtFU24lWSDbdYu2qldxzJlL
                r8E55ATsHwFBTezRwyTdsPNPS2F9Zx6fJme19WJMpkZDbImbxt8MxM1uI//6R0Xh
                RqCvfnEGXRlj049uSKdRHExOCvp7EWEwfFznJqjFVUHbivR2p6szlq4dLkuexZko
                EOSx2wWnzRW/NMg/5hqeLscbtyq7rjLfQTKcwTi4z8dymCHRf0RUzFX8ybgCt6y6
                fpy8eY4/AgMBAAECgf8O4Mc02ITyCxWNpBWVcF50HRZpoXODOndw9+0GHfBGDMDO
                kbmD+lltABO3zBBUdjguFjSF4HgkFviv8/O6WhcEne6kNZ1UhC9NvGKUJ1R5GVp3
                OWX3YTTvRGzwfBge/c+R49j1pSmnEUDrYrCA+gujNGMsUE+aZTd8N6nT0ZCrCHbW
                iGeAAzjWZbQ+tx/ygotYGUDG5qqtQwZl52krKQ3OlMlmltcNddrLg/dBMeHu8g+S
                bQivyd2QW0K9oglklreA6GnUUEOCt80hODIbBbpdyxVXcsDtNfffddqgf8j8xQm3
                pLUmufVW5gXlYnFogiSwKHPOvzmVPGyx/rBkmfkCgYEA4GSlIyE3vBSk+UkNa4zH
                1sxUGq+3fi0vEEMsZsoOpsPPL4UJnktHnwiBkBsH5dIhLY/2NY9OHtIzMPTYvvRc
                qG6RyQ61DTIjkVbNbfvpV4CAFnHRCh4ms/ZOukhegOfaRKNGADld1eUzsmShFTbV
                uOZQLeE+PyEvJegCRya+AVkCgYEA1R9WdxbUq5bEyiU8euIOe8jCfICADFVQ957+
                oZ4XMprXX/uNBa2IIEgkXW2uWYCL09fL+UlVcUu5iyVqXaRLBzD3/JW9IfCzCm1A
                xaMpezv1vLyZp0n0VvTN2uag6t5M/FkYCmp+m+VzQi82dFm0DuTFulOix78lYVHA
                wFKbwVcCgYByqdtczS+e02nN3M+XwrOnhnf/vwTj3BDtnXXF/MBp5SstHC1jDxLF
                KGKUkcuCW9MKZkMo8Va5Fy6DeMp9IX9rrjye4f4QhSt5rEKDTjPZu9c4IObx5aBf
                W6C1Ph/UfSWi50/w81+I2nuFUDikD4Y82qvkFfJp7foaw6jOVPTI2QKBgQC7kZQY
                xbgwuEXEH1eGUxQqL3uz9ag8so3LEVzLQwbpm8t4Bz2LRLnsp3GR5KkwzmjB7kfv
                w3H2f43x/+EIP0NlNdzbqbHGgEAjKhp6luo4MoJJNLgKupTYPyY5xQbVDwc0hPka
                mbWKYTu6gTDs39IP1ZqMLXWzVPCCIWCCI3I/iwKBgArrjiqmWQ55xRbdGCvw5RCv
                cIyuvwZTRYGrkGDIq7HHcFQDTqcxMMQhe0ox9WgQDuu8hAsrVcJv2Ia/14W3xQPT
                yGAc9EtS112c3ZOYCyuPY4NgoIad9TvpHvpiaLNukbIUAIimoNbyWAUwmkTjTjkC
                4X+WEhgfmekYzcrk6nfS
                -----END PRIVATE KEY-----
                """;
        var serviceAccountFileInJson = String.format("""
                {
                  "type": "service_account",
                  "project_id": "project-test-1",
                  "private_key_id": "id1",
                  "private_key": "%s",
                  "client_email": "test@project-test-1.iam.gserviceaccount.com",
                  "client_id": "client_id1",
                  "auth_uri": "https://accounts.google.com/o/oauth2/auth",
                  "token_uri": "https://oauth2.googleapis.com/token",
                  "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
                  "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/project-test-1.iam.gserviceaccount.com"
                }""", privateKey);

        var tokenInJson = """
                {"edctype":"dataspaceconnector:gcptoken","token":"18f4155e-4d02-4629-8804-057a3e9277ba","expiration":1665998128000}""";

        serviceAccountFileInB64 = Base64.getEncoder().encodeToString(serviceAccountFileInJson.getBytes());

        when(vault.resolveSecret(accessTokenKeyName)).thenReturn(tokenInJson);
        when(vault.resolveSecret(invalidAccessTokenKeyName)).thenReturn("{\"token\":\"" + tokenValue + "\"");
        when(vault.resolveSecret(serviceAccountKeyName)).thenReturn(serviceAccountFileInJson);
        when(vault.resolveSecret(serviceAccountKeyName)).thenReturn(serviceAccountFileInJson);
        when(vault.resolveSecret(invalidServiceAccountKeyName)).thenReturn("{\"type\": \"service_account\" }");
        gcpCredential = new GcpCredentials(vault, new TypeManager(), mock(Monitor.class));
    }

    @Test
    void testResolveGoogleCredentialWhenTokenKeyNameIsProvided() {
        var gcpServiceAccountCredentialsWithTokenKeyName = new GcpServiceAccountCredentials(accessTokenKeyName, null, null);
        var gcpCred = gcpCredential.resolveGoogleCredentialsFromDataAddress(gcpServiceAccountCredentialsWithTokenKeyName);
        assertThat(gcpCred).isNotNull();
    }

    @Test
    void testResolveGoogleCredentialWhenInvalidTokenIsProvided() {
        var gcpServiceAccountCredentialsWithInvalidTokenKeyName = new GcpServiceAccountCredentials(invalidAccessTokenKeyName,  null, null);
        assertThatThrownBy(() -> gcpCredential.resolveGoogleCredentialsFromDataAddress(gcpServiceAccountCredentialsWithInvalidTokenKeyName))
                .isInstanceOf(EdcException.class)
                .hasMessageContaining("valid GcpAccessToken format");
    }

    @Test
    void testResolveGoogleCredentialPriorityWhenTokenIsInvalid() {
        var gcpServiceAccountCredentialsWithInvalidTokenKeyName = new GcpServiceAccountCredentials(invalidAccessTokenKeyName, serviceAccountKeyName, null);
        assertThatThrownBy(() -> gcpCredential.resolveGoogleCredentialsFromDataAddress(gcpServiceAccountCredentialsWithInvalidTokenKeyName))
                .isInstanceOf(EdcException.class)
                .hasMessageContaining("valid GcpAccessToken format");
    }


    @Test
    void testResolveGoogleCredentialWhenServiceAccountKeyNameIsProvided() {
        var gcpServiceAccountCredentialsFromServiceAccountKeyName = new GcpServiceAccountCredentials(null, serviceAccountKeyName, null);
        var gcpCred = gcpCredential.resolveGoogleCredentialsFromDataAddress(
                gcpServiceAccountCredentialsFromServiceAccountKeyName
        );
        assertThat(gcpCred).isNotNull();
    }

    @Test
    void testResolveGoogleCredentialWhenInvalidServiceAccountKeyNameIsProvided() {
        var gcpServiceAccountCredentialsFromInvalidServiceAccountKeyName = new GcpServiceAccountCredentials(null, invalidServiceAccountKeyName, null);
        assertThatThrownBy(() -> gcpCredential.resolveGoogleCredentialsFromDataAddress(gcpServiceAccountCredentialsFromInvalidServiceAccountKeyName))
                .isInstanceOf(GcpException.class)
                .hasMessageContaining("Error while getting the credentials from the credentials file");
    }

    @Test
    void testResolveGoogleCredentialWhenServiceAccountValueIsProvided() {
        var gcpServiceAccountCredentialsFromB64ServiceAccount = new GcpServiceAccountCredentials(null, null, serviceAccountFileInB64);
        var gcpCred = gcpCredential.resolveGoogleCredentialsFromDataAddress(
                gcpServiceAccountCredentialsFromB64ServiceAccount
        );
        assertThat(gcpCred).isNotNull();
    }

    @Test
    void testResolveGoogleCredentialWhenInvalidServiceAccountValueIsProvided() {
        var invalidServiceAccountValue = serviceAccountFileInB64 + System.lineSeparator();
        var gcpServiceAccountCredentialsFromInvalidB64ServiceAccount = new GcpServiceAccountCredentials(null, null, invalidServiceAccountValue);
        assertThatThrownBy(() -> gcpCredential.resolveGoogleCredentialsFromDataAddress(gcpServiceAccountCredentialsFromInvalidB64ServiceAccount))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("Illegal base64 character");
    }

    @Test
    void testResolveGoogleCredentialPriorityWhenInvalidServiceAccountValueIsProvided() {
        var gcpServiceAccountCredentials = new GcpServiceAccountCredentials(null, serviceAccountKeyName, serviceAccountFileInB64 + "makeItWrongB64");
        var gcpCred = gcpCredential.resolveGoogleCredentialsFromDataAddress(
                gcpServiceAccountCredentials
        );
        assertThat(gcpCred).isNotNull();
    }
}