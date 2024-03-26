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

package org.eclipse.edc.gcp.bigquery.validation;

import org.eclipse.edc.gcp.bigquery.service.BigQueryService;
import org.eclipse.edc.runtime.metamodel.annotation.Extension;
import org.eclipse.edc.runtime.metamodel.annotation.Inject;
import org.eclipse.edc.spi.system.ServiceExtension;
import org.eclipse.edc.spi.system.ServiceExtensionContext;
import org.eclipse.edc.validator.spi.DataAddressValidatorRegistry;

@Extension(BigQueryDataAddressValidatorExtension.NAME)
public class BigQueryDataAddressValidatorExtension implements ServiceExtension {
    public static final String NAME = "BigQuery DataAddress Validator";

    @Inject
    private DataAddressValidatorRegistry validatorRegistry;

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public void initialize(ServiceExtensionContext context) {
        validatorRegistry.registerDestinationValidator(BigQueryService.BIGQUERY_DATA,
                new BigQuerySinkDataAddressValidator());
        validatorRegistry.registerSourceValidator(BigQueryService.BIGQUERY_DATA,
                new BigQuerySourceDataAddressValidator());
    }
}
