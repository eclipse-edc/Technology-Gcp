/*
 *  Copyright (c) 2023 Bayerische Motoren Werke Aktiengesellschaft (BMW AG)
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       Bayerische Motoren Werke Aktiengesellschaft (BMW AG) - initial API and implementation
 *
 */

rootProject.name = "Technology-Gcp"

pluginManagement {
    repositories {
        maven {
            url = uri("https://central.sonatype.com/repository/maven-snapshots/")
        }
        mavenCentral()
        gradlePluginPortal()
    }
}

include(":extensions:common:gcp:gcp-core")
include(":extensions:common:vault:vault-gcp")

include(":extensions:control-plane:provision:provision-gcs")
include(":extensions:control-plane:provision:provision-bigquery")

include(":extensions:data-plane:data-plane-google-storage")
include(":extensions:data-plane:data-plane-google-bigquery")

include(":system-tests:gcp-bigquery-transfer-tests")
include(":system-tests:runtimes:gcp-bigquery-transfer-consumer")
include(":system-tests:runtimes:gcp-bigquery-transfer-provider")

include(":version-catalog")
