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

import com.google.cloud.bigquery.BigQuery;
import org.eclipse.edc.gcp.common.GcpServiceAccount;

import java.io.IOException;

/**
 * Interface used to provide a specific factory for the provision service.
 * Can be used by test to provide mocks.
 */
public interface BigQueryFactory {
    /**
     * Provides instances of the BigQuery service using specific service account name.
     *
     * @param serviceAccount the service account to use to execute bigquery calls
     * @return the instance of the service
     */
    BigQuery createBigQuery(GcpServiceAccount serviceAccount) throws IOException;
}
