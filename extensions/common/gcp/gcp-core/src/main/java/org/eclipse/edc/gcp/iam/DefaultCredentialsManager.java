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
 *       Google LLC - Initial implementation
 *
 */

package org.eclipse.edc.gcp.iam;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.gcp.common.GcpServiceAccount;
import org.eclipse.edc.spi.monitor.Monitor;

import java.io.IOException;
import java.util.Arrays;

/**
 * The DefaultCredentialsManager class provides the implementation of the CredentialsManager
 * interface by means of the standard GCP API to fetch application-default credentials, refresh
 * credentials, and impersonate service accounts.
 */
record DefaultCredentialsManager(Monitor monitor) implements CredentialsManager {
    @Override
    public GoogleCredentials getApplicationDefaultCredentials() {
        try {
            return GoogleCredentials.getApplicationDefault();
        } catch (IOException ioException) {
            monitor.severe("Cannot get application default credentials", ioException);
            throw new GcpException(ioException);
        }
    }

    @Override
    public void refreshCredentials(GoogleCredentials credentials) {
        try {
            credentials.refreshIfExpired();
        } catch (IOException ioException) {
            monitor.severe("Cannot get refresh the credentials", ioException);
            throw new GcpException(ioException);
        }
    }

    @Override
    public GoogleCredentials createImpersonated(GoogleCredentials sourceCredentials, GcpServiceAccount serviceAccount, int lifeTime, String... scopes) {
        return ImpersonatedCredentials.create(
              sourceCredentials,
              serviceAccount.getEmail(),
              null,
              Arrays.asList(scopes),
              lifeTime);
    }
}
