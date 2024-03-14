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

/**
 * Interface used by BigQuery Provisioner.
 */
public interface BigQueryProvisionService {
    /**
     * Checks whether the table defined exists.
     *
     * @return true if the table targeted exists.
     */
    boolean tableExists();
}
