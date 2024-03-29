/*
 *  Copyright (c) 2022 Google LLC
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

package org.eclipse.edc.gcp.iam;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.services.iam.v2.IamScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.iam.admin.v1.IAMClient;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenRequest;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.iam.credentials.v1.ServiceAccountName;
import com.google.protobuf.Duration;
import org.eclipse.edc.gcp.common.GcpAccessToken;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.gcp.common.GcpServiceAccount;
import org.eclipse.edc.spi.monitor.Monitor;

import java.io.IOException;
import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

public class IamServiceImpl implements IamService {
    private static final long ONE_HOUR_IN_S = TimeUnit.HOURS.toSeconds(1);
    private final Monitor monitor;
    private final String gcpProjectId;
    private Supplier<IAMClient> iamClientSupplier;
    private Supplier<IamCredentialsClient> iamCredentialsClientSupplier;
    private AccessTokenProvider applicationDefaultCredentials;

    private IamServiceImpl(Monitor monitor, String gcpProjectId) {
        this.monitor = monitor;
        this.gcpProjectId = gcpProjectId;
    }

    @Override
    public GcpServiceAccount getServiceAccount(String serviceAccountName) {
        try (var client = iamClientSupplier.get()) {
            var serviceAccountEmail = getServiceAccountEmail(serviceAccountName, gcpProjectId);
            var name = ServiceAccountName.of(gcpProjectId, serviceAccountEmail).toString();
            var response = client.getServiceAccount(name);

            return new GcpServiceAccount(response.getEmail(), response.getName(), response.getDescription());
        } catch (ApiException e) {
            if (e.getStatusCode().getCode() == StatusCode.Code.NOT_FOUND) {
                monitor.severe("Service account '" + serviceAccountName + "'not found", e);
                throw new GcpException("Service account '" + serviceAccountName + "'not found", e);
            }
            monitor.severe("Unable to get service account '" + serviceAccountName + "'", e);
            throw new GcpException("Unable to get service account '" + serviceAccountName + "'", e);
        }
    }

    @Override
    public GcpAccessToken createAccessToken(GcpServiceAccount serviceAccount) {
        try (var iamCredentialsClient = iamCredentialsClientSupplier.get()) {
            var name = ServiceAccountName.of("-", serviceAccount.getEmail());
            var lifetime = Duration.newBuilder().setSeconds(ONE_HOUR_IN_S).build();
            var request = GenerateAccessTokenRequest.newBuilder()
                    .setName(name.toString())
                    .addAllScope(Collections.singleton(IamScopes.CLOUD_PLATFORM))
                    .setLifetime(lifetime)
                    .build();
            var response = iamCredentialsClient.generateAccessToken(request);
            monitor.debug("Created access token for " + serviceAccount.getEmail());
            var expirationMillis = response.getExpireTime().getSeconds() * 1000;
            return new GcpAccessToken(response.getAccessToken(), expirationMillis);
        } catch (Exception e) {
            throw new GcpException("Error creating service account token:\n" + e);
        }
    }

    @Override
    public GcpAccessToken createDefaultAccessToken() {
        return applicationDefaultCredentials.getAccessToken();
    }

    private String getServiceAccountEmail(String name, String project) {
        return String.format("%s@%s.iam.gserviceaccount.com", name, project);
    }

    public static class Builder {
        private IamServiceImpl iamServiceImpl;

        private Builder(Monitor monitor, String gcpProjectId) {
            iamServiceImpl = new IamServiceImpl(monitor, gcpProjectId);
        }

        public static IamServiceImpl.Builder newInstance(Monitor monitor, String gcpProjectId) {
            return new Builder(monitor, gcpProjectId);
        }

        public Builder iamClientSupplier(Supplier<IAMClient> iamClientSupplier) {
            iamServiceImpl.iamClientSupplier = iamClientSupplier;
            return this;
        }

        public Builder iamCredentialsClientSupplier(Supplier<IamCredentialsClient> iamCredentialsClientSupplier) {
            iamServiceImpl.iamCredentialsClientSupplier = iamCredentialsClientSupplier;
            return this;
        }

        public Builder applicationDefaultCredentials(AccessTokenProvider applicationDefaultCredentials) {
            iamServiceImpl.applicationDefaultCredentials = applicationDefaultCredentials;
            return this;
        }

        public IamServiceImpl build() {
            Objects.requireNonNull(iamServiceImpl.gcpProjectId, "gcpProjectId");
            Objects.requireNonNull(iamServiceImpl.monitor, "monitor");

            if (iamServiceImpl.iamClientSupplier == null) {
                iamServiceImpl.iamClientSupplier = defaultIamClientSupplier();
            }
            if (iamServiceImpl.iamCredentialsClientSupplier == null) {
                iamServiceImpl.iamCredentialsClientSupplier = defaultIamCredentialsClientSupplier();
            }

            if (iamServiceImpl.applicationDefaultCredentials == null) {
                iamServiceImpl.applicationDefaultCredentials = new ApplicationDefaultCredentials(iamServiceImpl.monitor);
            }

            return iamServiceImpl;
        }

        /**
         * Supplier of {@link IAMClient} using application default credentials
         */
        private Supplier<IAMClient> defaultIamClientSupplier() {
            return () -> {
                try {
                    return IAMClient.create();
                } catch (IOException e) {
                    throw new GcpException("Error while creating IAMClient", e);
                }
            };
        }

        /**
         * Supplier of {@link IamCredentialsClient} using application default credentials
         */
        private Supplier<IamCredentialsClient> defaultIamCredentialsClientSupplier() {
            return () -> {
                try {
                    return IamCredentialsClient.create();
                } catch (IOException e) {
                    throw new GcpException("Error while creating IamCredentialsClient", e);
                }
            };
        }
    }

    private record ApplicationDefaultCredentials(Monitor monitor) implements AccessTokenProvider {
        @Override
        public GcpAccessToken getAccessToken() {
            try {
                var credentials = GoogleCredentials.getApplicationDefault().createScoped(IamScopes.CLOUD_PLATFORM);
                credentials.refreshIfExpired();
                var token = credentials.getAccessToken();
                return new GcpAccessToken(token.getTokenValue(), token.getExpirationTime().getTime());
            } catch (IOException ioException) {
                monitor.severe("Cannot get application default access token", ioException);
                return null;
            }
        }
    }
}
