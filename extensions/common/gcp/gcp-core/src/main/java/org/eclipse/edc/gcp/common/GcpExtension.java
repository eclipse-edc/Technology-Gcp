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

import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Provider;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;

/**
 * GCP common extension, provides the GCP manager object.
 */
@Extension(value = GcpExtension.NAME)
public class GcpExtension implements ServiceExtension {
    private GcpManager gcpManager;
    public static final String NAME = "GCP";

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        gcpManager = new GcpManager(context);
    }

    @Provider
    public GcpManager getGcpManager() {
        return gcpManager;
    }
}
