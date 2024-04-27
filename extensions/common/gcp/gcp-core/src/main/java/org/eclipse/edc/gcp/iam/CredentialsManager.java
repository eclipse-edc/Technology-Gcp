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
import org.eclipse.edc.gcp.common.GcpServiceAccount;

/**
 * Interface for credentials providing access tokens.
 */
public interface CredentialsManager {
    /**
     * Returns the default credentials.
     *
     * @return the {@link GoogleCredentials}.
     */
    GoogleCredentials getApplicationDefaultCredentials();

    /**
     * Refresh the credentials if needed.
     *
     * @param credentials the credentials to be refreshed.
     */
    void refreshCredentials(GoogleCredentials credentials);

    /**
     * Returns the impersonated credentials.
     *
     * @param sourceCredentials the source credentials to start for impersonation.
     * @param serviceAccount the service account to be impersonated.
     * @param lifeTime lifetime of the credentials in seconds.
     * @param scopes the list of scopes to be added to the credentials.
     * @return the impersonated {@link GoogleCredentials}.
     */
    GoogleCredentials createImpersonated(GoogleCredentials sourceCredentials, GcpServiceAccount serviceAccount, int lifeTime, String... scopes);
}
