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
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Provider;
import org.eclipse.edc.runtime.metamodel.annotation.Setting;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;

/**
 * GCP common extension, provides the GCP manager object.
 */
@Extension(value = GcpExtension.NAME)
public class GcpExtension implements ServiceExtension {
    public static final String NAME = "GCP";
    @Setting(description = "Default GCP project ID for the connector", required = false)
    public static final String PROJECT_ID = "edc.gcp.project.id";

    @Setting(description = "Default service account name for the connector", required = false)
    public static final String SACCOUNT_NAME = "edc.gcp.saccount.name";

    @Setting(description = "Default JSON file with service account credentials for the connector", required = false)
    public static final String SACCOUNT_FILE = "edc.gcp.saccount.file";

    @Setting(description = "Default universe domain for the connector", required = false)
    public static final String UNIVERSE_DOMAIN = "edc.gcp.universe";

    private GcpConfiguration gcpConfiguration;


    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {

        var projectId = context.getSetting(PROJECT_ID, ServiceOptions.getDefaultProjectId());
        var serviceAccountName = context.getSetting(SACCOUNT_NAME, null);
        var serviceAccountFile = context.getSetting(SACCOUNT_FILE, null);
        var universeDomain = context.getSetting(UNIVERSE_DOMAIN, null);

        gcpConfiguration = new GcpConfiguration(projectId, serviceAccountName, serviceAccountFile, universeDomain);
    }

    @Provider
    public GcpConfiguration getGcpConfiguration() {
        return gcpConfiguration;
    }

}
