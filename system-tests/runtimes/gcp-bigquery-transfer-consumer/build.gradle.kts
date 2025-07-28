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
 *       Google LLC - Initial implementation
 *
 */

plugins {
  `java-library`
  id("application")
  alias(libs.plugins.shadow)
}

dependencies {
  implementation(libs.edc.core.runtime)
  implementation(libs.edc.core.connector)
  implementation(libs.edc.core.edrstore)
  implementation(libs.edc.control.plane.core)
  implementation(libs.edc.control.plane.api.client)
  implementation(libs.edc.dsp)
  implementation(libs.edc.http)
  implementation(libs.edc.configuration.filesystem)
  implementation(libs.edc.iam.mock)
  implementation(libs.edc.management.api)
  implementation(libs.edc.data.plane.self.registration)
  implementation(libs.edc.transfer.data.plane.signaling)
  implementation(libs.edc.control.plane.api)
  implementation(libs.edc.auth.spi)
  implementation(libs.edc.control.api.configuration)

  implementation(libs.edc.data.plane.selector.api)
  implementation(libs.edc.data.plane.selector.core)

  implementation(libs.edc.data.plane.core)
  implementation(libs.edc.data.plane.http)
  implementation(libs.edc.data.plane.util)

  implementation(project(":extensions:data-plane:data-plane-google-bigquery"))
}

application {
  mainClass.set("org.eclipse.edc.boot.system.runtime.BaseRuntime")
}

tasks.withType<com.github.jengelman.gradle.plugins.shadow.tasks.ShadowJar> {
  mergeServiceFiles()
  archiveFileName.set("consumer.jar")
}

edcBuild {
  publish.set(false)
}
