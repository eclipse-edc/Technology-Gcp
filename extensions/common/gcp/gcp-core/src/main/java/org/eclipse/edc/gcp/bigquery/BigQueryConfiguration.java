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

/**
 * Configuration class embedding the common GcpConfiguration and BigQuery-specific parameters.
 */
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

    /**
     * The common GCP configuration.
     *
     * @return the GcpConfiguration object.
     */
    public GcpConfiguration gcpConfiguration() {
        return gcpConfiguration;
    }

    /**
     * The BigQuery REST API endpoint host if defined in the EDC configuration or, if not
     * found in the EDC configuration, as defined in the system properties "edc.gcp.bq.rest".
     *
     * @return the REST endpoint as http://host:port.
     */
    public String restEndpoint() {
        if (restEndpoint != null) {
            return restEndpoint;
        }

        return ConfigurationFunctions.propOrEnv("edc.gcp.bq.rest", null);
    }

    /**
     * The BigQuery Storage RPC API endpoint host if defined in the EDC configuration or, if not
     * found in the EDC configuration, as defined in the system properties "edc.gcp.bq.rpc".
     *
     * @return the RPC endpoint as http://host:port.
     */
    public String rpcEndpoint() {
        if (rpcEndpoint != null) {
            return rpcEndpoint;
        }

        return ConfigurationFunctions.propOrEnv("edc.gcp.bq.rpc", null);
    }

    /**
     * The thread pool size used to prepare the ExecutorService for the BigQuery source.
     *
     * @return the number of threads in the pool.
     */
    public int threadPoolSize() {
        return threadPoolSize;
    }
}

