/*
 *  Copyright (c) 2023 Google LLC
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

plugins {
    `java-library`
}

dependencies {
    api(libs.edc.spi.core)

    implementation(libs.edc.util)
    implementation(libs.googlecloud.core)
    implementation(libs.googlecloud.secretmanager)

    implementation(project(":extensions:common:gcp:gcp-core"))
}
