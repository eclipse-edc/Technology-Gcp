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

package org.eclipse.edc.gcp.iam;


import org.eclipse.edc.gcp.common.GcpAccessToken;
import org.eclipse.edc.gcp.common.GcpServiceAccount;

/**
 * Wrapper around GCP IAM-API for decoupling.
 */
public interface IamService {
    /**
     * Returns the existing service account with the matching name.
     *
     * @param serviceAccountName        the name for the service account. Limited to 30 chars
     * @return the {@link GcpServiceAccount} describing the service account
     */
    GcpServiceAccount getServiceAccount(String serviceAccountName);

    /**
     * Creates a temporary valid OAunth2.0 access token for the service account
     *
     * @param serviceAccount The service account the token should be created for
     * @return {@link GcpAccessToken}
     */
    GcpAccessToken createAccessToken(GcpServiceAccount serviceAccount);

    /**
     * Creates a temporary valid OAunth2.0 access token using the application default account credentials.
     *
     * @return {@link GcpAccessToken}
     */
    GcpAccessToken createDefaultAccessToken();
}