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

import org.eclipse.edc.gcp.common.GcpAccessToken;

/**
 * Interface for credentials providing access tokens.
 */
public interface AccessTokenProvider {
    /**
     * Returns the access token generated for the credentials.
     *
     * @return the {@link GcpAccessToken} for the credentials, null if error occurs.
     */
    GcpAccessToken getAccessToken();
}
