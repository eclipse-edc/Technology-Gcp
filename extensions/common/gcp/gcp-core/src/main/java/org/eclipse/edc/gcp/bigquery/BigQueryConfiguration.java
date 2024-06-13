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
 *       Google LLC
 *
 */

package org.eclipse.edc.gcp.bigquery;

import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.util.configuration.ConfigurationFunctions;

public class BigQueryConfiguration {
    private GcpConfiguration gcpConfiguration;
    private String restEndpoint;
    private String rpcEndpoint;
    private int threadPoolSize;


    public BigQueryConfiguration(GcpConfiguration gcpConfiguration,
                                 String restEndpoint, String rpcEndpoint, int threadPoolSize) {
        this.gcpConfiguration = gcpConfiguration;
        this.restEndpoint = restEndpoint;
        this.rpcEndpoint = rpcEndpoint;
        this.threadPoolSize = threadPoolSize;
    }

    public BigQueryConfiguration(GcpConfiguration gcpConfiguration) {
        this(gcpConfiguration, null, null, 0);
    }

    public GcpConfiguration gcpConfiguration() {
        return gcpConfiguration;
    }

    public String restEndpoint() {
        if (restEndpoint != null) {
            return restEndpoint;
        }

        return ConfigurationFunctions.propOrEnv("edc.gcp.bq.rest", null);
    }

    public String rpcEndpoint() {
        if (rpcEndpoint != null) {
            return rpcEndpoint;
        }

        return ConfigurationFunctions.propOrEnv("edc.gcp.bq.rpc", null);
    }

    public int threadPoolSize() {
        return threadPoolSize;
    }
}

