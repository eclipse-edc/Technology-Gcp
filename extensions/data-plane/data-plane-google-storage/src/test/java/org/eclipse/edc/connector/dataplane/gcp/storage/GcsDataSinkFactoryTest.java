/*
 *  Copyright (c) 2022 T-Systems International GmbH
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       T-Systems International GmbH
 *
 */

package org.eclipse.edc.connector.dataplane.gcp.storage;

import com.google.auth.oauth2.GoogleCredentials;
import org.eclipse.edc.gcp.common.GcpAccessToken;
import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.gcp.storage.GcsStoreSchema;
import org.eclipse.edc.json.JacksonTypeManager;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.security.Vault;
import org.eclipse.edc.spi.types.TypeManager;
import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.spi.types.domain.transfer.DataFlowStartMessage;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.ArgumentsProvider;
import org.junit.jupiter.params.provider.ArgumentsSource;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class GcsDataSinkFactoryTest {
    private final TypeManager typeManager = new JacksonTypeManager();
    private Vault vault = mock();
    private IamService iamService = mock();
    private GcsDataSinkFactory factory = new GcsDataSinkFactory(
            mock(ExecutorService.class),
            mock(Monitor.class),
            vault,
            typeManager,
            iamService
        );

    @Test
    void canHandle_returnsTrueWhenExpectedType() {
        var destination = DataAddress.Builder
                .newInstance()
                .type(GcsStoreSchema.TYPE)
                .build();

        var result = factory.canHandle(createRequest(destination));

        assertThat(result).isTrue();
    }

    @Test
    void canHandle_returnsFalseWhenUnexpectedType() {
        var destination = DataAddress.Builder
                .newInstance()
                .type("Not Google Storage")
                .build();

        var result = factory.canHandle(createRequest(destination));

        assertThat(result).isFalse();
    }

    @Test
    void validate_ShouldSucceedIfPropertiesAreValid() {
        var destination = DataAddress.Builder
                .newInstance()
                .type(GcsStoreSchema.TYPE)
                .property(GcsStoreSchema.BUCKET_NAME, "validBucketName")
                .property(GcsStoreSchema.BLOB_NAME, "validBlobName")
                .build();

        var request = createRequest(destination);

        var result = factory.validateRequest(request);

        assertThat(result.succeeded()).isTrue();
    }

    @ParameterizedTest
    @ArgumentsSource(InvalidInputProvider.class)
    void validate_shouldFailIfPropertiesAreMissing(String bucketName) {
        var destination = DataAddress.Builder
                .newInstance()
                .type(GcsStoreSchema.TYPE)
                .property(GcsStoreSchema.BUCKET_NAME, bucketName)
                .build();

        var request = createRequest(destination);

        var result = factory.validateRequest(request);

        assertThat(result.failed()).isTrue();
    }

    @Test
    void createSink_ShouldSucceedIfRequestIsValidWithAdc() {
        var destination = DataAddress.Builder
                .newInstance()
                .type(GcsStoreSchema.TYPE)
                .property(GcsStoreSchema.BUCKET_NAME, "validBucketName")
                .property(GcsStoreSchema.BLOB_NAME, "validBlobName")
                .build();

        var request = createRequest(destination);
        var credentials = mock(GoogleCredentials.class);
        when(iamService.getCredentials(IamService.ADC_SERVICE_ACCOUNT, IamService.GCS_SCOPE)).thenReturn(credentials);
        var dataSink = factory.createSink(request);

        assertThat(dataSink).isNotNull();
    }

    @Test
    void createSink_ShouldSucceedIfRequestIsValidWithAccessToken() {
        var destination = DataAddress.Builder
                .newInstance()
                .type(GcsStoreSchema.TYPE)
                .property(DataAddress.EDC_DATA_ADDRESS_KEY_NAME, "testKeyName")
                .property(GcsStoreSchema.BUCKET_NAME, "validBucketName")
                .property(GcsStoreSchema.BLOB_NAME, "validBlobName")
                .build();

        var request = createRequest(destination);
        var credentials = mock(GoogleCredentials.class);
        var gcpAccessToken = new GcpAccessToken("tokenValue", 1000);
        var accessToken = typeManager.writeValueAsString(gcpAccessToken);

        when(vault.resolveSecret("testKeyName")).thenReturn(accessToken);
        when(iamService.getCredentials(any(GcpAccessToken.class))).thenReturn(credentials);
        var dataSink = factory.createSink(request);

        assertThat(dataSink).isNotNull();
    }

    @Test
    void createSink_ShouldThrowExceptionIfRequestIsInvalid() {
        // Destination is missing the bucket.
        var destination = DataAddress.Builder
                .newInstance()
                .type(GcsStoreSchema.TYPE)
                .property(DataAddress.EDC_DATA_ADDRESS_KEY_NAME, "testKeyName")
                .property(GcsStoreSchema.BLOB_NAME, "validBlobName")
                .build();

        var request = createRequest(destination);
        assertThatThrownBy(() -> factory.createSink(request)).isInstanceOf(EdcException.class);
    }

    private DataFlowStartMessage createRequest(DataAddress destination) {
        var source = DataAddress.Builder
                .newInstance()
                .type(GcsStoreSchema.TYPE)
                .build();

        return DataFlowStartMessage.Builder.newInstance()
                .id(UUID.randomUUID().toString())
                .processId(UUID.randomUUID().toString())
                .sourceDataAddress(source)
                .destinationDataAddress(destination)
                .build();
    }

    private static class InvalidInputProvider implements ArgumentsProvider {
        @Override
        public Stream<Arguments> provideArguments(ExtensionContext context) {
            return Stream.of(
                    Arguments.of(""),
                    Arguments.of(" ")
            );
        }
    }
}
