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

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.util.string.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Date;
import java.util.Objects;

public class GcpCredentials {

    public enum GcpCredentialType {
        DEFAULT_APPLICATION, GOOGLE_ACCESS_TOKEN, GOOGLE_SERVICE_ACCOUNT_KEY_FILE
    }

    private static final String SERVICE_ACCOUNT = "service_account";
    private final Base64.Decoder b64Decoder;
    private final Vault vault;
    private final TypeManager typeManager;
    private final Monitor monitor;

    public GcpCredentials(Vault vault, TypeManager typeManager, Monitor monitor) {
        this.vault = vault;
        this.typeManager = typeManager;
        this.b64Decoder = Base64.getDecoder();
        this.monitor = monitor;
    }

    /**
     * Returns the Google Credentials which will be created based on the following order:
     * if none of the  parameters were provided then Google Credentials will be retrieved from ApplicationDefaultCredentials
     * Otherwise it will be retrieved from a token or a Google Credentials file
     *
     * @return GoogleCredentials
     */
    public GoogleCredentials resolveGoogleCredentialsFromDataAddress(GcpServiceAccountCredentials gcpServiceAccountCredentials) {
        var vaultTokenKeyName = gcpServiceAccountCredentials.getVaultTokenKeyName();
        var vaultServiceAccountKeyName = gcpServiceAccountCredentials.getVaultServiceAccountKeyName();
        var serviceAccountValue = gcpServiceAccountCredentials.getServiceAccountValue();

        if (!StringUtils.isNullOrBlank(vaultTokenKeyName)) {
            var token = vault.resolveSecret(vaultTokenKeyName);
            if (StringUtils.isNullOrEmpty(token)) {
                throw new GcpException(vaultTokenKeyName + " could not be retrieved from the vault.");
            }
            return createGoogleCredential(token, GcpCredentialType.GOOGLE_ACCESS_TOKEN);
        } else if (!StringUtils.isNullOrBlank(vaultServiceAccountKeyName)) {
            var token = vault.resolveSecret(vaultServiceAccountKeyName);
            if (StringUtils.isNullOrEmpty(token)) {
                throw new GcpException(vaultServiceAccountKeyName + " could not be retrieved from the vault.");
            }
            return createGoogleCredential(token, GcpCredentialType.GOOGLE_SERVICE_ACCOUNT_KEY_FILE);
        } else if (!StringUtils.isNullOrBlank(serviceAccountValue)) {
            var serviceKeyContent = new String(b64Decoder.decode(serviceAccountValue));
            if (!serviceKeyContent.contains(SERVICE_ACCOUNT)) {
                throw new GcpException("SERVICE_ACCOUNT_VALUE is not provided as a valid service account key file.");
            }
            return createGoogleCredential(serviceKeyContent, GcpCredentialType.GOOGLE_SERVICE_ACCOUNT_KEY_FILE);
        } else {
            return createApplicationDefaultCredentials();
        }
    }

    /**
     * Returns the Google Credentials which will created based on the Application Default Credentials in the following approaches
     * - Credentials file pointed to by the GOOGLE_APPLICATION_CREDENTIALS environment variable
     * - Credentials provided by the Google Cloud SDK gcloud auth application-default login command
     * - Google App Engine built-in credentials
     * - Google Cloud Shell built-in credentials
     * - Google Compute Engine built-in credentials
     *
     * @return GoogleCredentials
     */
    public GoogleCredentials createApplicationDefaultCredentials() {
        return createGoogleCredential("", GcpCredentialType.DEFAULT_APPLICATION);
    }


    public GoogleCredentials createGoogleCredential(String keyContent, GcpCredentialType gcpCredentialType) {
        Objects.requireNonNull(keyContent, "key content");

        return switch (gcpCredentialType) {
            case GOOGLE_ACCESS_TOKEN -> getGoogleCredentialsFromAccessToken(keyContent);
            case GOOGLE_SERVICE_ACCOUNT_KEY_FILE -> getGoogleCredentialsFromFile(keyContent);
            case DEFAULT_APPLICATION -> getGoogleCredentialsFromApplicationDefault();
        };
    }

    private GoogleCredentials getGoogleCredentialsFromApplicationDefault() {
        try {
            monitor.debug("Gcp: The default Credentials will be used to resolve the google credentials.");
            return GoogleCredentials.getApplicationDefault();
        } catch (IOException e) {
            throw new GcpException("Error while getting the default credentials.", e);
        }
    }

    private GoogleCredentials getGoogleCredentialsFromFile(String keyContent) {
        try {
            monitor.debug("Gcp: The provided credentials file will be used to resolve the google credentials.");
            return GoogleCredentials.fromStream(new ByteArrayInputStream(keyContent.getBytes(StandardCharsets.UTF_8)));
        } catch (IOException e) {
            throw new GcpException("Error while getting the credentials from the credentials file.", e);
        }
    }

    private GoogleCredentials getGoogleCredentialsFromAccessToken(String keyContent) {
        if (StringUtils.isNullOrEmpty(keyContent)) {
            throw new GcpException("keyContent is not in a valid GcpAccessToken format.");
        }
        try {
            var gcpAccessToken = typeManager.readValue(keyContent, GcpAccessToken.class);
            monitor.info("Gcp: The provided token will be used to resolve the google credentials.");
            return  GoogleCredentials.create(
                    new AccessToken(gcpAccessToken.getToken(),
                            new Date(gcpAccessToken.getExpiration())));
        } catch (EdcException ex) {
            throw new GcpException("ACCESS_TOKEN is not in a valid GcpAccessToken format.");
        } catch (Exception e) {
            throw new GcpException("Error while getting the default credentials.", e);
        }
    }
}