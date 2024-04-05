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

plugins {
    `java-library`
}

dependencies {
    api(libs.edc.spi.core)
    api(libs.edc.spi.dataplane)
    api(libs.edc.spi.transfer)
    api(libs.edc.lib.util)
    implementation(libs.edc.spi.validator)


    // GCP dependencies.
    implementation(platform(libs.googlecloud.bom))
    implementation(libs.googlecloud.bigquery)
    implementation(libs.googlecloud.iam.admin)
    implementation(libs.googlecloud.storage)
    implementation(libs.googlecloud.iam.credentials)
    implementation(libs.googleapis.iam)

    testImplementation(libs.edc.junit)
    testImplementation(libs.googlecloud.gax)
}


