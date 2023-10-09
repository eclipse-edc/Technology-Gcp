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
 *       Google LCC - Initial implementation
 *
 */

package org.eclipse.edc.gcp.common;

import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.gcp.iam.IamServiceImpl;
import org.eclipse.edc.spi.system.ServiceExtensionContext;

/**
 * GCP manager object, provides connector configuration, IAM credentials.
 */
public class GcpManager {
    private final ServiceExtensionContext context;
    private final GcpConfiguration configuration;
    private IamService iamService;

    public GcpManager(ServiceExtensionContext context) {
        this.context = context;
        configuration = new GcpConfiguration(context);
    }

    public ServiceExtensionContext getContext() {
        return context;
    }

    public GcpConfiguration getConfiguration() {
        return configuration;
    }

    public IamService getIamService() {
        if (iamService == null) {
            iamService = IamServiceImpl.Builder.newInstance(context.getMonitor(), getConfiguration().getProjectId()).build();
        }

        return iamService;
    }
}
