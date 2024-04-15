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
 *       Google LLC - Initial implementation
 *
 */

package org.eclipse.edc.gcp.common;

/**
 * Common configuration of the connector, provides accessors to parameters.
 */
public record GcpConfiguration(String projectId, String serviceAccountName, String serviceAccountFile,
                               String universeDomain) {

    /**
     * Project ID for the connector.
     *
     * @return the default project ID of the connector, or the default from the cloud SDK.
     */
    public String projectId() {
        return projectId;
    }

    /**
     * Service account name for the connector.
     *
     * @return the default service account name of the connector, or null if not available.
     */
    public String serviceAccountName() {
        return serviceAccountName;
    }

    /**
     * Service account JSON key file for the connector.
     *
     * @return the default service account key file path of the connector, or null if not available.
     */
    public String serviceAccountFile() {
        return serviceAccountFile;
    }

    /**
     * Universe domain for the connector.
     *
     * @return the default universe domain of the connector, or null if not available.
     */
    public String universeDomain() {
        return universeDomain;
    }
}
