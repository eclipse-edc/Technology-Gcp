
plugins {
  `java-library`
  `java-test-fixtures`
}

dependencies {
  testFixturesApi(project(":extensions:common:gcp:gcp-core"))
  testFixturesApi(libs.edc.jsonld)
  testFixturesApi(testFixtures(libs.edc.management.api.test.fixtures))
  testFixturesApi(libs.edc.lib.util)
  testFixturesApi(libs.restAssured)
  testFixturesImplementation(libs.edc.junit)
  testFixturesImplementation(libs.assertj)

  api(libs.edc.controlplane.spi)
  testFixturesApi(libs.edc.spi.transaction.datasource)
  testFixturesApi(libs.testcontainers.junit)
  testFixturesApi(libs.testcontainers.gcloud)
  testFixturesApi(libs.edc.lib.util)
  testFixturesApi(libs.edc.junit)
  testFixturesApi(libs.edc.sql.core)
  testFixturesApi(libs.junit.jupiter.api)

  // GCP dependencies.
  testFixturesApi(platform(libs.googlecloud.bom))
  testFixturesApi(libs.googlecloud.bigquery)
  testFixturesApi(libs.googlecloud.iam.admin)
  testFixturesApi(libs.googlecloud.storage)
  testFixturesApi(libs.googlecloud.iam.credentials)
  testFixturesApi(libs.googleapis.iam)

  testRuntimeOnly(project(":system-tests:runtimes:gcp-bigquery-transfer-provider"))
  testRuntimeOnly(project(":system-tests:runtimes:gcp-bigquery-transfer-consumer"))
}

edcBuild {
  publish.set(false)
}
