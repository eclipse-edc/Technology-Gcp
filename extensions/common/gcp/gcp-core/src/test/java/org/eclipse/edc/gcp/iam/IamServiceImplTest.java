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
 *       Google LLC - Initial implementation
 *
 */

package org.eclipse.edc.gcp.iam;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.iam.admin.v1.IAMClient;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenRequest;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenResponse;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.iam.credentials.v1.ServiceAccountName;
import com.google.iam.admin.v1.ServiceAccount;
import com.google.protobuf.Duration;
import com.google.protobuf.Timestamp;
import org.eclipse.edc.gcp.common.GcpAccessToken;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.gcp.common.GcpServiceAccount;
import org.eclipse.edc.spi.monitor.Monitor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class IamServiceImplTest {
    private final String testTokenValue = "test-access-token";
    private final long testTokenExpirationTime = 433454334;
    private final String testScopes = "test-scopes";
    private final String projectId = "test-project-Id";
    private final String serviceAccountName = "test-service-account";
    private final String serviceAccountEmail = String.format("%s@%s.iam.gserviceaccount.com", serviceAccountName, projectId);
    private final String cfgServiceAccountName = "cfg-service-account";
    private final String cfgServiceAccountEmail =  String.format("%s@%s.iam.gserviceaccount.com", cfgServiceAccountName, projectId);
    private final String serviceAccountDescription = "service-account-description";
    private IamService iamApi;
    private IAMClient iamClient;
    private IamCredentialsClient iamCredentialsClient;
    private CredentialsManager credentialsManager;
    private GcpServiceAccount testServiceAccount;
    private GcpConfiguration gcpConfiguration;

    @BeforeEach
    void setUp() {
        var monitor = mock(Monitor.class);
        iamClient = mock();
        iamCredentialsClient = mock();
        credentialsManager = mock();
        gcpConfiguration = mock();
        testServiceAccount = new GcpServiceAccount(serviceAccountEmail, serviceAccountName, serviceAccountDescription);

        iamApi = IamServiceImpl.Builder.newInstance(monitor, gcpConfiguration)
                .iamClientSupplier(() -> iamClient)
                .iamCredentialsClientSupplier(() -> iamCredentialsClient)
                .credentialUtil(credentialsManager)
                .build();

        var credentialAccessToken = AccessToken.newBuilder()
                .setTokenValue(testTokenValue)
                .setExpirationTime(new Date(testTokenExpirationTime))
                .build();
        var googleCredentials = mock(GoogleCredentials.class);
        when(credentialsManager.getApplicationDefaultCredentials())
                .thenReturn(googleCredentials);
        when(googleCredentials.createScoped(any(String[].class))).thenReturn(googleCredentials);
        when(googleCredentials.getAccessToken()).thenReturn(credentialAccessToken);
        when(credentialsManager.createImpersonated(
            any(GoogleCredentials.class),
            any(GcpServiceAccount.class),
            anyInt(),
            any(String[].class))).thenAnswer(i -> i.getArguments()[0]);
    }

    @Test
    void testGetServiceAccount() {
        var name = ServiceAccountName.of(projectId, serviceAccountEmail).toString();
        var serviceAccount = ServiceAccount.newBuilder()
                .setEmail(serviceAccountEmail)
                .setDescription(serviceAccountDescription)
                .build();
        when(iamClient.getServiceAccount(name)).thenReturn(serviceAccount);
        when(gcpConfiguration.projectId()).thenReturn(projectId);

        var createdServiceAccount = iamApi.getServiceAccount(serviceAccountName);

        assertThat(createdServiceAccount.getEmail()).isEqualTo(serviceAccountEmail);
        assertThat(createdServiceAccount.getDescription()).isEqualTo(serviceAccountDescription);
    }

    @Test
    void testGetConfigServiceAccount() {
        var name = ServiceAccountName.of(projectId, cfgServiceAccountEmail).toString();
        var serviceAccount = ServiceAccount.newBuilder()
                .setName(cfgServiceAccountName)
                .setEmail(cfgServiceAccountEmail)
                .setDescription(serviceAccountDescription)
                .build();
        when(iamClient.getServiceAccount(name)).thenReturn(serviceAccount);
        when(gcpConfiguration.projectId()).thenReturn(projectId);
        when(gcpConfiguration.serviceAccountName()).thenReturn(cfgServiceAccountName);

        var returnedServiceAccount = iamApi.getServiceAccount(null);

        assertThat(returnedServiceAccount.getEmail()).isEqualTo(cfgServiceAccountEmail);
        assertThat(returnedServiceAccount.getName()).isEqualTo(cfgServiceAccountName);
        assertThat(returnedServiceAccount.getDescription()).isEqualTo(serviceAccountDescription);
    }

    @Test
    void testGetDefaultServiceAccount() {
        var returnedServiceAccount = iamApi.getServiceAccount(null);

        assertThat(returnedServiceAccount).isEqualTo(IamService.ADC_SERVICE_ACCOUNT);
    }

    @Test
    void testGetServiceAccountThatDoesntExist() {
        var name = ServiceAccountName.of(projectId, serviceAccountEmail).toString();
        var getError = apiExceptionWithStatusCode(StatusCode.Code.NOT_FOUND);
        when(iamClient.getServiceAccount(name)).thenThrow(getError);
        when(gcpConfiguration.projectId()).thenReturn(projectId);

        assertThatThrownBy(() -> iamApi.getServiceAccount(serviceAccountName)).isInstanceOf(GcpException.class);
    }

    @Test
    void testCreateAccessToken() {
        long timeout = 3600;
        var expectedKey = GenerateAccessTokenResponse.newBuilder()
                .setAccessToken(testTokenValue)
                .setExpireTime(Timestamp.newBuilder().setSeconds(timeout))
                .build();
        String[] scope = {};
        var expectedRequest = GenerateAccessTokenRequest.newBuilder()
                .setName("projects/-/serviceAccounts/" + serviceAccountEmail)
                .addAllScope(Arrays.asList(scope))
                .setLifetime(Duration.newBuilder().setSeconds(TimeUnit.HOURS.toSeconds(1)).build())
                .build();
        when(iamCredentialsClient.generateAccessToken(expectedRequest)).thenReturn(expectedKey);

        var accessToken = iamApi.createAccessToken(testServiceAccount);

        assertThat(accessToken.getToken()).isEqualTo(testTokenValue);
        assertThat(accessToken.getExpiration()).isEqualTo(timeout * 1000);
    }

    @Test
    void testCreateAccessTokenWithScope() {
        long timeout = 3600;
        var expectedKey = GenerateAccessTokenResponse.newBuilder()
                .setAccessToken(testTokenValue)
                .setExpireTime(Timestamp.newBuilder().setSeconds(timeout))
                .build();
        var scope = "test_scope";
        var expectedRequest = GenerateAccessTokenRequest.newBuilder()
                .setName("projects/-/serviceAccounts/" + serviceAccountEmail)
                .addAllScope(Arrays.asList(scope))
                .setLifetime(Duration.newBuilder().setSeconds(TimeUnit.HOURS.toSeconds(1)).build())
                .build();

        when(iamCredentialsClient.generateAccessToken(expectedRequest)).thenReturn(expectedKey);

        var accessToken = iamApi.createAccessToken(testServiceAccount, scope);

        assertThat(accessToken.getToken()).isEqualTo(testTokenValue);
        assertThat(accessToken.getExpiration()).isEqualTo(timeout  * 1000);
    }

    @Test
    void testCreateDefaultAccessToken() {
        var accessToken = iamApi.createAccessToken(IamService.ADC_SERVICE_ACCOUNT);
        assertThat(accessToken.getToken()).isEqualTo(testTokenValue);
        assertThat(accessToken.getExpiration()).isEqualTo(testTokenExpirationTime);
    }

    @Test
    void testCreateDefaultAccessTokenWithScope() {
        var scope = "test_scope";

        var accessToken = iamApi.createAccessToken(IamService.ADC_SERVICE_ACCOUNT, scope);
        assertThat(accessToken.getToken()).isEqualTo(testTokenValue);
        assertThat(accessToken.getExpiration()).isEqualTo(testTokenExpirationTime);
    }

    @Test
    void testCreateDefaultAccessTokenError() {
        when(credentialsManager.getApplicationDefaultCredentials()).thenThrow(new GcpException("Cannot get credentials"));
        assertThatThrownBy(() -> iamApi.createAccessToken(IamService.ADC_SERVICE_ACCOUNT)).isInstanceOf(GcpException.class);
    }

    @Test
    void testGetCredentialsFromTokenSucceeds() {
        var expectedTokenString = "test-access-token";
        long timeout = 3600;
        var passedAccessToken = new GcpAccessToken(expectedTokenString, timeout);
        var credentials = iamApi.getCredentials(passedAccessToken);
        assertThat(credentials).isNotNull().extracting(GoogleCredentials::getAccessToken).satisfies(accessToken -> {
            assertThat(accessToken.getTokenValue()).isEqualTo(expectedTokenString);
            assertThat(accessToken.getExpirationTime()).isEqualTo(Instant.ofEpochMilli(timeout));
        });
    }

    @Test
    void testGetCredentialsWithAdcSucceeds() {
        var credentials = iamApi.getCredentials(IamService.ADC_SERVICE_ACCOUNT, testScopes);
        assertThat(credentials).isNotNull().extracting(GoogleCredentials::getAccessToken).satisfies(accessToken -> {
            assertThat(accessToken.getTokenValue()).isEqualTo(testTokenValue);
            assertThat(accessToken.getExpirationTime()).isEqualTo(new Date(testTokenExpirationTime));
        });
    }

    @Test
    void testGetCredentialsWithServiceAccountSucceeds() {
        var serviceAccount = new GcpServiceAccount(serviceAccountEmail, serviceAccountName, serviceAccountDescription);
        var credentials = iamApi.getCredentials(serviceAccount, testScopes);
        assertThat(credentials).isNotNull().extracting(GoogleCredentials::getAccessToken).satisfies(accessToken -> {
            assertThat(accessToken.getTokenValue()).isEqualTo(testTokenValue);
            assertThat(accessToken.getExpirationTime()).isEqualTo(new Date(testTokenExpirationTime));
        });
    }

    private ApiException apiExceptionWithStatusCode(StatusCode.Code code) {
        return ApiExceptionFactory.createException(
                new Exception(), new StatusCode() {
                    @Override
                    public Code getCode() {
                        return code;
                    }

                    @Override
                    public Object getTransportCode() {
                        return null;
                    }
                }, false);
    }
}
