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

import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.gcp.iam.IamServiceImpl;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Provider;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;

/**
 * GCP common extension, provides the GCP manager object.
 */
@Extension(value = GcpExtension.NAME)
public class GcpExtension implements ServiceExtension {
    public static final String NAME = "GCP";

    private GcpConfiguration gcpConfiguration;
    private IamService iamService;


    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        gcpConfiguration = new GcpConfiguration(context);
        iamService = IamServiceImpl.Builder.newInstance(context.getMonitor(), gcpConfiguration.getProjectId()).build();
    }

    @Provider
    public GcpConfiguration getGcpConfiguration() {
        return gcpConfiguration;
    }

    @Provider
    public IamService getIamService() {
        return iamService;
    }
}
