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
 *       Google LLC - Initial implementation
 *
 */

package org.eclipse.edc.gcp.iam;


import com.google.auth.oauth2.GoogleCredentials;
import org.eclipse.edc.gcp.common.GcpAccessToken;
import org.eclipse.edc.gcp.common.GcpServiceAccount;

/**
 * Wrapper around GCP IAM-API for decoupling.
 */
public interface IamService {
    /**
     * Service account representing Application Default Credentials.
     */
    GcpServiceAccount ADC_SERVICE_ACCOUNT = new GcpServiceAccount("adc-email", "adc-name", "application default");
    /**
     * OAUTH2 scope for BigQuery access.
     */
    String BQ_SCOPE = "https://www.googleapis.com/auth/bigquery";
    /**
     * OAUTH2 scope for GCS read/write access.
     */
    String GCS_SCOPE = "https://www.googleapis.com/auth/devstorage.read_write";
    /**
     * OAUTH2 scope for IAM access, used to then impersonate a service account.
     */
    String IAM_SCOPE = "https://www.googleapis.com/auth/iam";

    /**
     * Returns the existing service account with the matching name.
     *
     * @param serviceAccountName        the name for the service account. Limited to 30 chars
     * @return the {@link GcpServiceAccount} describing the service account
     */
    GcpServiceAccount getServiceAccount(String serviceAccountName);

    /**
     * Creates a temporary valid OAuth2.0 access token for the service account
     *
     * @param serviceAccount service account the token should be created for;
     *                       if ADC_SERVICE_ACCOUNT, access token from ADC is created.
     * @param scopes list of scopes to be requested for the access token, see
     *               https://developers.google.com/identity/protocols/oauth2/scopes
     * @return {@link GcpAccessToken}
     */
    GcpAccessToken createAccessToken(GcpServiceAccount serviceAccount, String... scopes);

    /**
     * Generates the credentials from a temporary valid OAuth2.0 access token
     *
     * @param accessToken token created by e.g. provisioner
     * @return {@link GoogleCredentials} corresponding to the given access token
     */
    GoogleCredentials getCredentials(GcpAccessToken accessToken);

    /**
     * Generates the credentials for a service account
     *
     * @param serviceAccount service account for the credentials, or null, to get default credentials
     * @param scopes list of scopes to be requested for the access token, see
     *               https://developers.google.com/identity/protocols/oauth2/scopes
     * @return {@link GoogleCredentials} corresponding to the give service account
     */
    GoogleCredentials getCredentials(GcpServiceAccount serviceAccount, String... scopes);
}
