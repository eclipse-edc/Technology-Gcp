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
import com.google.api.gax.rpc.ApiExceptionFactory;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.iam.admin.v1.IAMClient;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenRequest;
import com.google.cloud.iam.credentials.v1.GenerateAccessTokenResponse;
import com.google.cloud.iam.credentials.v1.IamCredentialsClient;
import com.google.cloud.iam.credentials.v1.ServiceAccountName;
import com.google.iam.admin.v1.CreateServiceAccountRequest;
import com.google.iam.admin.v1.ServiceAccount;
import org.eclipse.edc.gcp.common.GcpAccessToken;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.gcp.common.GcpServiceAccount;
import org.eclipse.edc.spi.monitor.Monitor;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.AdditionalMatchers.and;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IamServiceImplTest {

    private final String projectId = "test-project-Id";
    private final String serviceAccountName = "test-service-account";
    private final String serviceAccountEmail = String.format("%s@%s.iam.gserviceaccount.com", serviceAccountName, projectId);
    private final String serviceAccountDescription = "service-account-description";
    private IamService iamApi;
    private IAMClient iamClient;
    private IamCredentialsClient iamCredentialsClient;

    private IamServiceImpl.ApplicationDefaultCredentials applicationDefaultCredentials;
    private GcpServiceAccount testServiceAccount;
    private final String iamServiceAccountName = "projects/" + projectId + "/serviceAccounts/" + serviceAccountEmail;

    @BeforeEach
    void setUp() {
        var monitor = Mockito.mock(Monitor.class);
        iamClient = Mockito.mock(IAMClient.class);
        iamCredentialsClient = Mockito.mock(IamCredentialsClient.class);
        applicationDefaultCredentials = Mockito.mock(IamServiceImpl.ApplicationDefaultCredentials.class);
        testServiceAccount = new GcpServiceAccount(serviceAccountEmail, serviceAccountName, serviceAccountDescription);
        iamApi = IamServiceImpl.Builder.newInstance(monitor, projectId)
                .iamClientSupplier(() -> iamClient)
                .iamCredentialsClientSupplier(() -> iamCredentialsClient)
                .applicationDefaultCredentials(() -> applicationDefaultCredentials)
                .build();
    }

    @Test
    void testCreateServiceAccount() {
        var serviceAccount = ServiceAccount.newBuilder().setEmail(serviceAccountEmail).build();
        when(iamClient.createServiceAccount(any(CreateServiceAccountRequest.class))).thenReturn(serviceAccount);

        GcpServiceAccount createdServiceAccount = iamApi.getOrCreateServiceAccount(serviceAccountName, serviceAccountDescription);

        assertThat(createdServiceAccount.getEmail()).isEqualTo(serviceAccountEmail);
        assertThat(createdServiceAccount.getDescription()).isEqualTo(serviceAccountDescription);
    }

    @Test
    void testCreateServiceAccountThatExistsAlreadyWithMatchingDescription() {
        var serviceAccount = ServiceAccount.newBuilder().setEmail(serviceAccountEmail).setDescription(serviceAccountDescription).build();
        var createError = apiExceptionWithStatusCode(StatusCode.Code.ALREADY_EXISTS);
        when(iamClient.createServiceAccount(any(CreateServiceAccountRequest.class))).thenThrow(createError);
        when(iamClient.getServiceAccount(eq(iamServiceAccountName))).thenReturn(serviceAccount);

        var createdServiceAccount = iamApi.getOrCreateServiceAccount(serviceAccountName, serviceAccountDescription);

        assertThat(createdServiceAccount.getEmail()).isEqualTo(serviceAccountEmail);
        assertThat(createdServiceAccount.getDescription()).isEqualTo(serviceAccountDescription);
    }

    @Test
    void testCreateServiceAccountThatExistsAlreadyWithOtherDescription() {
        var serviceAccount = ServiceAccount.newBuilder().setEmail(serviceAccountEmail).setDescription("some-other-description").build();
        var createError = apiExceptionWithStatusCode(StatusCode.Code.ALREADY_EXISTS);
        when(iamClient.createServiceAccount(any(CreateServiceAccountRequest.class))).thenThrow(createError);
        when(iamClient.getServiceAccount(eq(iamServiceAccountName))).thenReturn(serviceAccount);

        assertThatThrownBy(() -> iamApi.getOrCreateServiceAccount(serviceAccountName, serviceAccountDescription)).isInstanceOf(GcpException.class);
    }

    @Test
    void testGetServiceAccount() {
        var name = ServiceAccountName.of(projectId, serviceAccountEmail).toString();
        var serviceAccount = ServiceAccount.newBuilder()
                .setEmail(serviceAccountEmail)
                .setDescription(serviceAccountDescription)
                .build();
        when(iamClient.getServiceAccount(name)).thenReturn(serviceAccount);

        var createdServiceAccount = iamApi.getServiceAccount(serviceAccountName);

        assertThat(createdServiceAccount.getEmail()).isEqualTo(serviceAccountEmail);
        assertThat(createdServiceAccount.getDescription()).isEqualTo(serviceAccountDescription);
    }

    @Test
    void testGetServiceAccountThatDoesntExist() {
        var name = ServiceAccountName.of(projectId, serviceAccountEmail).toString();
        var serviceAccount = ServiceAccount.newBuilder()
                .setEmail(serviceAccountEmail)
                .setDescription(serviceAccountDescription)
                .build();
        var getError = apiExceptionWithStatusCode(StatusCode.Code.NOT_FOUND);
        when(iamClient.getServiceAccount(name)).thenThrow(getError);

        assertThatThrownBy(() -> iamApi.getServiceAccount(serviceAccountName)).isInstanceOf(GcpException.class);
    }

    @Test
    void testCreateAccessToken() {
        var expectedTokenString = "test-access-token";
        var expectedKey = GenerateAccessTokenResponse.newBuilder().setAccessToken(expectedTokenString).build();
        GenerateAccessTokenRequest expectedRequest = and(
                argThat(x -> x.getName().equals("projects/-/serviceAccounts/" + serviceAccountEmail)),
                argThat(x -> x.getLifetime().getSeconds() == 3600)
        );
        when(iamCredentialsClient.generateAccessToken(expectedRequest)).thenReturn(expectedKey);

        var accessToken = iamApi.createAccessToken(testServiceAccount);

        assertThat(accessToken.getToken()).isEqualTo(expectedTokenString);
    }

    @Test
    void testCreateDefaultAccessToken() {
        var expectedTokenString = "test-access-token";
        long timeout = 3600;
        when(applicationDefaultCredentials.getAccessToken()).thenReturn(new GcpAccessToken(expectedTokenString, timeout));

        var accessToken = iamApi.createDefaultAccessToken();
        assertThat(accessToken.getToken()).isEqualTo(expectedTokenString);
        assertThat(accessToken.getExpiration()).isEqualTo(timeout);
    }

    @Test
    void testCreateDefaultAccessTokenError() {
        when(applicationDefaultCredentials.getAccessToken()).thenReturn(null);

        var accessToken = iamApi.createDefaultAccessToken();
        assertThat(accessToken).isNull();
    }

    @Test
    void testDeleteServiceAccount() {
        var serviceAccount = new GcpServiceAccount(serviceAccountEmail, serviceAccountName, serviceAccountDescription);
        doNothing().when(iamClient).deleteServiceAccount(serviceAccountName);

        iamApi.deleteServiceAccountIfExists(serviceAccount);

        verify(iamClient, times(1)).deleteServiceAccount(iamServiceAccountName);
    }

    @Test
    void testDeleteServiceAccountThatAlreadyExistsSucceeds() {
        var exception = apiExceptionWithStatusCode(StatusCode.Code.NOT_FOUND);
        doThrow(exception)
                .when(iamClient).deleteServiceAccount(serviceAccountName);

        iamApi.deleteServiceAccountIfExists(testServiceAccount);

        verify(iamClient, times(1)).deleteServiceAccount(iamServiceAccountName);
    }

    @Test
    void testDeleteServiceAccountFails() {
        var exception = apiExceptionWithStatusCode(StatusCode.Code.INTERNAL);

        doThrow(exception)
                .when(iamClient).deleteServiceAccount(iamServiceAccountName);

        assertThatThrownBy(() -> iamApi.deleteServiceAccountIfExists(testServiceAccount))
                .isInstanceOf(GcpException.class);
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
