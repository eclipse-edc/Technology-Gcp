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

package org.eclipse.edc.gcp.bigquery;

import org.eclipse.edc.junit.extensions.DependencyInjectionExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
// import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.validator.spi.DataAddressValidatorRegistry;
// import org.eclipse.edc.validator.spi.ValidationResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

@ExtendWith(DependencyInjectionExtension.class)
public class BigQueryDataAddressValidatorExtensionTest {
    private static final String TEST_PROJECT = "test-project";
    private static final String TEST_DATASET = "test-dataset";
    private static final String TEST_TABLE = "test-table";
    private static final String TEST_QUERY = "SELECT * from " + TEST_TABLE;
    private static final String TEST_OTHER_TYPE = "AnotherDataAddressType";
    private static final String TEST_CUSTOMER_NAME = "customer-name";
    private static final String TEST_SINK_SERVICE_ACCOUNT_NAME = "sinkAccount";
    private final DataAddressValidatorRegistry registry = mock();

    @BeforeEach
    void setupContext(ServiceExtensionContext context) {
        context.registerService(DataAddressValidatorRegistry.class, registry);
    }

    @Test
    void testInitializeShouldRegisterValidators(BigQueryDataAddressValidatorExtension extension, ServiceExtensionContext context) {
        extension.initialize(context);

        assertThat(extension.name()).isEqualTo(BigQueryDataAddressValidatorExtension.NAME);

        verify(registry).registerDestinationValidator(eq(BigQueryService.BIGQUERY_DATA), isA(BigQuerySinkDataAddressValidator.class));
        verify(registry).registerSourceValidator(eq(BigQueryService.BIGQUERY_DATA), isA(BigQuerySourceDataAddressValidator.class));
    }

    /*
    @Test
    void testExtensionShouldValidateWellFormedAddresses(BigQueryDataAddressValidatorExtension extension, ServiceExtensionContext context) {
        extension.initialize(context);

        var validSourceAddress = DataAddress.Builder.newInstance()
                .type(BigQueryService.BIGQUERY_DATA)
                .property(BigQueryService.QUERY, TEST_QUERY)
                .build();

        // assertThat(registry.validateSource(validSourceAddress)).isEqualTo(ValidationResult.success());

        var validSinkAddress = DataAddress.Builder.newInstance()
                .type(BigQueryService.BIGQUERY_DATA)
                .property(BigQueryService.DATASET, TEST_DATASET)
                .property(BigQueryService.TABLE, TEST_TABLE)
                .build();

        assertThat(registry.validateDestination(validSinkAddress)).isEqualTo(ValidationResult.success());
    }

    @Test
    void testExtensionShouldNotValidateIncompleteAddresses(BigQueryDataAddressValidatorExtension extension, ServiceExtensionContext context) {
        extension.initialize(context);

        var invalidSourceAddressNoQuery = DataAddress.Builder.newInstance()
                .type(BigQueryService.BIGQUERY_DATA)
                .property(BigQueryService.TABLE, TEST_TABLE)
                .build();

        assertThat(registry.validateSource(invalidSourceAddressNoQuery)).isNotEqualTo(ValidationResult.success());

        var invalidSinkAddressNoTable = DataAddress.Builder.newInstance()
                .type(BigQueryService.BIGQUERY_DATA)
                .property(BigQueryService.DATASET, TEST_DATASET)
                .build();

        assertThat(registry.validateDestination(invalidSinkAddressNoTable)).isNotEqualTo(ValidationResult.success());

        var invalidSinkAddressNoDataset = DataAddress.Builder.newInstance()
                .type(BigQueryService.BIGQUERY_DATA)
                .property(BigQueryService.TABLE, TEST_TABLE)
                .build();

        assertThat(registry.validateDestination(invalidSinkAddressNoDataset)).isNotEqualTo(ValidationResult.success());
    }
    */
}
