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

plugins {
    `java-library`
}

dependencies {
    api(libs.edc.spi.dataplane)
    api(libs.edc.core.dataplane.util)

    implementation(project(":extensions:common:gcp:gcp-core"))
    implementation(libs.edc.spi.validator)

    testImplementation(libs.edc.core.dataplane)
    testImplementation(libs.edc.junit)
    testImplementation(libs.edc.keys.spi)

    // GCP dependencies.
    implementation(platform(libs.googlecloud.bom))
    implementation(libs.googlecloud.bigquery)
}
