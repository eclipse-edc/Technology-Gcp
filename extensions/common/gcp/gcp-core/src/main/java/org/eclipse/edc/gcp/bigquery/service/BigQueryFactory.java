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

package org.eclipse.edc.gcp.bigquery.service;

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.spi.monitor.Monitor;

import java.io.IOException;

/**
 * Interface used to provide a specific factory for the provision service.
 * Can be used by test to provide mocks.
 */
public interface BigQueryFactory {
    /**
     * Provides instances of the BigQuery service using specific credentials.
     *
     * @param gcpConfiguration connector configuration
     * @param credentials the credentials to use to execute bigquery calls
     * @param monitor monitor object for logging
     * @return the instance of the service
     */
    BigQuery createBigQuery(GcpConfiguration gcpConfiguration, GoogleCredentials credentials, Monitor monitor);

    /**
     * Provides instances of the BigQuery service using specific service account name.
     *
     * @param gcpConfiguration connector configuration
     * @param serviceAccountName the service account to use to execute bigquery calls
     * @param monitor monitor object for logging
     * @return the instance of the service
     */
    BigQuery createBigQuery(GcpConfiguration gcpConfiguration, String serviceAccountName, Monitor monitor) throws IOException;

    /**
     * Provides instances of the BigQuery service using default credentials and service account from configuration.
     *
     * @param gcpConfiguration connector configuration
     * @param monitor monitor object for logging
     * @return the instance of the service
     */
    BigQuery createBigQuery(GcpConfiguration gcpConfiguration, Monitor monitor) throws IOException;
}
