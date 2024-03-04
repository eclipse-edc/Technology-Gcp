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

import com.google.cloud.ServiceOptions;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.system.ServiceExtensionContext;

/**
 * Common configuration of the connector, provides accessors to parameters.
 */
public class GcpConfiguration {
    @Setting(value = "Default GCP project ID for the connector", required = false)
    public static final String PROJECT_ID = "edc.gcp.project.id";

    @Setting(value = "Default service account name for the connector", required = false)
    public static final String SACCOUNT_NAME = "edc.gcp.saccount.name";

    @Setting(value = "Default JSON file with service account credentials for the connector", required = false)
    public static final String SACCOUNT_FILE = "edc.gcp.saccount.file";

    @Setting(value = "Default universe domain for the connector", required = false)
    public static final String UNIVERSE_DOMAIN = "edc.gcp.universe";

    private String projectId;
    private String serviceAccountName;
    private String serviceAccountFile;
    private String universeDomain;

    public GcpConfiguration(ServiceExtensionContext context) {
        projectId = context.getSetting(PROJECT_ID, ServiceOptions.getDefaultProjectId());
        serviceAccountName = context.getSetting(SACCOUNT_NAME, null);
        serviceAccountFile = context.getSetting(SACCOUNT_FILE, null);
        universeDomain = context.getSetting(UNIVERSE_DOMAIN, null);
    }

    /**
     * Project ID for the connector.
     *
     * @return the default project ID of the connector, or the default from the cloud SDK.
     */
    public String getProjectId() {
        return projectId;
    }

    /**
     * Service account name for the connector.
     *
     * @return the default service account name of the connector, or an empty string if not available.
     */
    public String getServiceAccountName() {
        return serviceAccountName;
    }

    /**
     * Service account JSON key file for the connector.
     *
     * @return the default service account key file path of the connector, or an empty string if not available.
     */
    public String getServiceAccountFile() {
        return serviceAccountFile;
    }

    /**
     * Universe domain for the connector.
     *
     * @return the default universe domain of the connector, or an empty string if not available.
     */
    public String getUniverseDomain() {
        return universeDomain;
    }
}
