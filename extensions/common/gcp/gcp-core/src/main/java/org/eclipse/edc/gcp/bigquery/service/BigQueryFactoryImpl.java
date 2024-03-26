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

import com.google.api.services.iam.v2.IamScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.spi.monitor.Monitor;

import java.io.IOException;
import java.util.Arrays;

public class BigQueryFactoryImpl implements BigQueryFactory {
    private GcpConfiguration gcpConfiguration;
    private String project;
    private Monitor monitor;

    @Override
    public BigQuery createBigQuery(GcpConfiguration gcpConfiguration, GoogleCredentials credentials, Monitor monitor) {
        return BigQueryOptions.newBuilder()
                .setProjectId(gcpConfiguration.projectId())
                .setCredentials(credentials)
                .build().getService();
    }

    @Override
    public BigQuery createBigQuery(GcpConfiguration gcpConfiguration, String serviceAccountName, Monitor monitor) throws IOException {
        var credentials = GoogleCredentials.getApplicationDefault()
                .createScoped(IamScopes.CLOUD_PLATFORM);
        credentials.refreshIfExpired();

        if (serviceAccountName == null) {
            serviceAccountName = gcpConfiguration.serviceAccountName();
        }

        if (serviceAccountName != null) {
            monitor.debug("BigQuery Service for project '" + gcpConfiguration.projectId() +
                    "' using service account '" + serviceAccountName + "'");
            credentials = ImpersonatedCredentials.create(
                    credentials,
                    serviceAccountName,
                    null,
                    Arrays.asList("https://www.googleapis.com/auth/bigquery"),
                    3600);
        } else {
            monitor.warning("BigQuery Service for project '" + gcpConfiguration.projectId() + "' using ADC, NOT RECOMMENDED");
        }

        return createBigQuery(gcpConfiguration, credentials, monitor);
    }

    @Override
    public BigQuery createBigQuery(GcpConfiguration gcpConfiguration, Monitor monitor) throws IOException {
        return createBigQuery(gcpConfiguration, gcpConfiguration.serviceAccountName(), monitor);
    }
}
