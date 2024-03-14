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
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import org.eclipse.edc.gcp.bigquery.BigQueryTarget;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.spi.monitor.Monitor;

import java.io.IOException;
import java.util.Arrays;
import java.util.Objects;

public class BigQueryProvisionServiceImpl implements BigQueryProvisionService {
    private final GcpConfiguration gcpConfiguration;
    private final BigQueryTarget target;
    private final Monitor monitor;
    private BigQuery bigQuery;
    private GoogleCredentials credentials;
    private String serviceAccountName;

    @Override
    public boolean tableExists() {
        try {
            var table = bigQuery.getTable(target.getTableId());
            return table != null && table.exists();
        } catch (BigQueryException bigQueryException) {
            monitor.debug(bigQueryException.toString());
            return false;
        }
    }

    public static class Builder {
        private final BigQueryProvisionServiceImpl bqProvisionService;

        public static Builder newInstance(GcpConfiguration gcpConfiguration, BigQueryTarget target, Monitor monitor) {
            return new Builder(gcpConfiguration, target, monitor);
        }

        private Builder(GcpConfiguration gcpConfiguration, BigQueryTarget target, Monitor monitor) {
            bqProvisionService = new BigQueryProvisionServiceImpl(gcpConfiguration, target, monitor);
        }

        public Builder credentials(GoogleCredentials credentials) {
            bqProvisionService.credentials = credentials;
            return this;
        }

        public Builder serviceAccount(String serviceAccountName) {
            bqProvisionService.serviceAccountName = serviceAccountName;
            return this;
        }

        Builder bigQuery(BigQuery bigQuery) {
            bqProvisionService.bigQuery = bigQuery;
            return this;
        }

        public BigQueryProvisionServiceImpl build() {
            try {
                bqProvisionService.initService();
                Objects.requireNonNull(bqProvisionService.bigQuery, "bigQuery");
                return bqProvisionService;
            } catch (IOException ioException) {
                throw new GcpException(ioException);
            }
        }
    }

    private BigQueryProvisionServiceImpl(GcpConfiguration gcpConfiguration, BigQueryTarget target, Monitor monitor) {
        this.gcpConfiguration = gcpConfiguration;
        this.target = target;
        this.monitor = monitor;
    }

    private void initCredentials() throws IOException {
        var project = target.project();
        if (project == null) {
            project = gcpConfiguration.getProjectId();
        }

        if (credentials != null) {
            monitor.debug("BigQuery Service for project '" + project + "' using provided credentials");
            return;
        }

        var sourceCredentials = GoogleCredentials.getApplicationDefault()
                .createScoped(IamScopes.CLOUD_PLATFORM);
        sourceCredentials.refreshIfExpired();

        if (serviceAccountName == null) {
            monitor.warning("BigQuery Service for project '" + project + "' using ADC, NOT RECOMMENDED");
            credentials = sourceCredentials;
            return;
        }

        monitor.debug("BigQuery Service for project '" + project + "' using service account '" + serviceAccountName + "'");
        credentials = ImpersonatedCredentials.create(
              sourceCredentials,
              serviceAccountName,
              null,
              Arrays.asList("https://www.googleapis.com/auth/bigquery"),
              3600);
    }

    private void initService() throws IOException {
        if (bigQuery != null) {
            return;
        }

        initCredentials();

        var project = target.project();
        if (project == null) {
            project = gcpConfiguration.getProjectId();
        }

        var bqBuilder = BigQueryOptions.newBuilder().setProjectId(project);

        if (credentials != null) {
            bqBuilder.setCredentials(credentials);
        }

        bigQuery = bqBuilder.build().getService();
    }

    private void refreshBigQueryCredentials() {
        var credentials = bigQuery.getOptions().getCredentials();
        if (credentials instanceof OAuth2Credentials authCredentials) {
            try {
                // TODO check margin and refresh if too close to expire.
                authCredentials.refreshIfExpired();
            } catch (IOException ioException) {
                monitor.warning("BigQuery Service cannot refresh credentials", ioException);
            }
        }
    }
}
