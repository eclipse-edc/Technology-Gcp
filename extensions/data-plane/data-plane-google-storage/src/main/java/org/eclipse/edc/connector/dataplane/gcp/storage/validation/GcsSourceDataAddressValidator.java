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

package org.eclipse.edc.connector.dataplane.gcp.storage.validation;

import org.eclipse.edc.spi.types.domain.DataAddress;
import org.eclipse.edc.validator.spi.ValidationResult;
import org.eclipse.edc.validator.spi.Validator;
import org.eclipse.edc.validator.spi.Violation;

import java.util.ArrayList;

import static org.eclipse.edc.gcp.storage.GcsStoreSchema.BLOB_NAME;
import static org.eclipse.edc.gcp.storage.GcsStoreSchema.BUCKET_NAME;
import static org.eclipse.edc.validator.spi.ValidationResult.failure;
import static org.eclipse.edc.validator.spi.ValidationResult.success;

public class GcsSourceDataAddressValidator implements Validator<DataAddress> {

    @Override
    public ValidationResult validate(DataAddress input) {
        var violations = new ArrayList<Violation>();
        if (StringFunctions.isEmpty(input.getStringProperty(BLOB_NAME, null))) {
            violations.add(Violation.violation("Must have a %s property".formatted(BLOB_NAME), BLOB_NAME));
        }
        if (StringFunctions.isEmpty(input.getStringProperty(BUCKET_NAME, null))) {
            violations.add(Violation.violation("Must have a %s property".formatted(BUCKET_NAME), BUCKET_NAME));
        }

        if (!violations.isEmpty()) {
            return failure(violations);
        }
        return success();
    }
}
