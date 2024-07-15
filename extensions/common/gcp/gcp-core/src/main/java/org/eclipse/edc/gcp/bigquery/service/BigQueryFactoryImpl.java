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
import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import org.eclipse.edc.gcp.bigquery.BigQueryConfiguration;
import org.eclipse.edc.gcp.common.GcpServiceAccount;
import org.eclipse.edc.gcp.iam.IamService;

public class BigQueryFactoryImpl implements BigQueryFactory {
    private final BigQueryConfiguration configuration;
    private final IamService iamService;

    public BigQueryFactoryImpl(BigQueryConfiguration configuration, IamService iamService) {
        this.configuration = configuration;
        this.iamService = iamService;
    }

    @Override
    public BigQuery createBigQuery(GcpServiceAccount serviceAccount) {
        var credentials = iamService.getCredentials(serviceAccount,
                IamService.BQ_SCOPE);
        return createBigQuery(credentials);
    }

    private BigQuery createBigQuery(GoogleCredentials credentials) {
        var bqBuilder = BigQueryOptions.newBuilder();
        var host = configuration.restEndpoint();
        if (host != null) {
            bqBuilder.setHost(host);
            bqBuilder.setLocation(host);
            bqBuilder.setCredentials(NoCredentials.getInstance());
        } else {
            bqBuilder.setCredentials(credentials);
        }
        return bqBuilder.build().getService();
    }
}
