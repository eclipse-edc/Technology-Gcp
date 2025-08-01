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

package org.eclipse.edc.test.system.bigquery;

import com.google.cloud.NoCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValue.Attribute;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.JobId;
import com.google.cloud.bigquery.JobInfo;
import com.google.cloud.bigquery.QueryJobConfiguration;
import com.google.cloud.bigquery.TableId;
import org.eclipse.edc.connector.controlplane.test.system.utils.PolicyFixtures;
import org.eclipse.edc.connector.controlplane.transfer.spi.types.TransferProcessStates;
import org.eclipse.edc.junit.annotations.ComponentTest;
import org.eclipse.edc.junit.extensions.EmbeddedRuntime;
import org.eclipse.edc.junit.extensions.RuntimeExtension;
import org.eclipse.edc.junit.extensions.RuntimePerMethodExtension;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.testcontainers.containers.BigQueryEmulatorContainer;
import org.testcontainers.containers.BindMode;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.testcontainers.shaded.org.awaitility.Awaitility.await;

@ComponentTest
@Testcontainers
public class BigQueryTransferIntegrationTest  {
    private static final List<FieldValueList> TEST_ROWS = Arrays.asList(FieldValueList.of(Arrays.asList(
            FieldValue.of(Attribute.PRIMITIVE, "1"),
            FieldValue.of(Attribute.PRIMITIVE, "name1"),
            FieldValue.of(Attribute.REPEATED,
                FieldValueList.of(Arrays.asList(
                    FieldValue.of(Attribute.RECORD,
                        FieldValueList.of(Arrays.asList(
                                FieldValue.of(Attribute.PRIMITIVE, "info1"),
                                FieldValue.of(Attribute.PRIMITIVE, "{\"age\": 56}")
                            )
                        ))))),
            FieldValue.of(Attribute.PRIMITIVE, "1970-01-01"),
            FieldValue.of(Attribute.PRIMITIVE, null),
            FieldValue.of(Attribute.PRIMITIVE, "43.200000")
        )),
            FieldValueList.of(Arrays.asList(
                FieldValue.of(Attribute.PRIMITIVE, "2"),
                FieldValue.of(Attribute.PRIMITIVE, "name2"),
                FieldValue.of(Attribute.REPEATED,
                    FieldValueList.of(Arrays.asList(
                        FieldValue.of(Attribute.RECORD,
                            FieldValueList.of(Arrays.asList(
                                    FieldValue.of(Attribute.PRIMITIVE, "info2"),
                                    FieldValue.of(Attribute.PRIMITIVE, "{\"age\": 34}")
                                )
                            ))))),
                FieldValue.of(Attribute.PRIMITIVE, "1970-01-01"),
                FieldValue.of(Attribute.PRIMITIVE, null),
                FieldValue.of(Attribute.PRIMITIVE, "64.800000")
        )),
            FieldValueList.of(Arrays.asList(
                FieldValue.of(Attribute.PRIMITIVE, "3"),
                FieldValue.of(Attribute.PRIMITIVE, "name3"),
                FieldValue.of(Attribute.REPEATED,
                    FieldValueList.of(Arrays.asList(
                        FieldValue.of(Attribute.RECORD,
                            FieldValueList.of(Arrays.asList(
                                    FieldValue.of(Attribute.PRIMITIVE, "info3"),
                                    FieldValue.of(Attribute.PRIMITIVE, "{\"age\": 33}")
                                )
                            ))))),
                FieldValue.of(Attribute.PRIMITIVE, "1970-01-01"),
                FieldValue.of(Attribute.PRIMITIVE, null),
                FieldValue.of(Attribute.PRIMITIVE, "64.800000")
                )),
            FieldValueList.of(Arrays.asList(
                FieldValue.of(Attribute.PRIMITIVE, "4"),
                FieldValue.of(Attribute.PRIMITIVE, "name4"),
                FieldValue.of(Attribute.REPEATED,
                    FieldValueList.of(Arrays.asList(
                        FieldValue.of(Attribute.RECORD,
                            FieldValueList.of(Arrays.asList(
                                    FieldValue.of(Attribute.PRIMITIVE, "info4"),
                                    FieldValue.of(Attribute.PRIMITIVE, "{\"age\": 44}")
                                )
                            ))))),
                FieldValue.of(Attribute.PRIMITIVE, "1970-01-01"),
                FieldValue.of(Attribute.PRIMITIVE, null),
                FieldValue.of(Attribute.PRIMITIVE, "64.800000")
            )),
            FieldValueList.of(Arrays.asList(
                FieldValue.of(Attribute.PRIMITIVE, "5"),
                FieldValue.of(Attribute.PRIMITIVE, "name5"),
                FieldValue.of(Attribute.REPEATED,
                    FieldValueList.of(Arrays.asList(
                        FieldValue.of(Attribute.RECORD,
                            FieldValueList.of(Arrays.asList(
                                    FieldValue.of(Attribute.PRIMITIVE, "info5"),
                                    FieldValue.of(Attribute.PRIMITIVE, "{\"age\": 55}")
                                )
                            ))))),
                FieldValue.of(Attribute.PRIMITIVE, "1970-01-01"),
                FieldValue.of(Attribute.PRIMITIVE, null),
                FieldValue.of(Attribute.PRIMITIVE, "64.800000")
            )));

    @Container
    private static final BigQueryEmulatorContainer BQ_CONTAINER = new BigQueryEmulatorContainer("ghcr.io/goccy/bigquery-emulator")
            .withFileSystemBind("../data/", "/data", BindMode.READ_ONLY)
            .withCommand(
                    "--project", "edc-test-project",
                    "--data-from-yaml", "/data/data.yaml");

    private final BigQueryTransferParticipant consumerClient = BigQueryTransferParticipant.Builder.newInstance()
            .id("consumer")
            .name("consumer")
            .build();

    private final BigQueryTransferParticipant providerClient = BigQueryTransferParticipant.Builder.newInstance()
            .id("provider")
            .name("provider")
            .build();

    @RegisterExtension
    private final RuntimeExtension bqProvider = new RuntimePerMethodExtension(
            new EmbeddedRuntime("provider", ":system-tests:runtimes:gcp-bigquery-transfer-provider")
                    .configurationProvider(providerClient::getConfig));

    @RegisterExtension
    private final RuntimeExtension bqConsumer = new RuntimePerMethodExtension(
            new EmbeddedRuntime("consumer", ":system-tests:runtimes:gcp-bigquery-transfer-consumer")
                    .configurationProvider(consumerClient::getConfig));

    @Test
    void transferTable_success() {
        assertTrue(BQ_CONTAINER.isRunning());

        var ports = BQ_CONTAINER.getExposedPorts();
        System.setProperty("EDC_GCP_BQ_REST", "http://localhost:" + BQ_CONTAINER.getMappedPort(ports.get(0)));
        System.setProperty("EDC_GCP_BQ_RPC", "http://localhost:" + BQ_CONTAINER.getMappedPort(ports.get(1)));

        System.setProperty("edc.gcp.bq.rest", "http://localhost:" + BQ_CONTAINER.getMappedPort(ports.get(0)));
        System.setProperty("edc.gcp.bq.rpc", "http://localhost:" + BQ_CONTAINER.getMappedPort(ports.get(1)));

        var assetId = providerClient.createBigQueryAsset(
                "Test asset",
                "edc-test-project",
                "edc-test-dataset",
                "sample_model",
                "table_temp_1",
                """
#standardSQL
SELECT * from edc-test-dataset.table_src;
                """);

        var policyId = providerClient.createPolicyDefinition(PolicyFixtures.noConstraintPolicy());
        providerClient.createContractDefinition(assetId, UUID.randomUUID().toString(), policyId, policyId);
        var transferProcessId = consumerClient.requestAssetAndTransferToBigQuery(providerClient, assetId,
                "edc-test-project", "edc-test-dataset", "table_dst");

        Duration pollInterval = Duration.ofSeconds(4);
        Duration timeout = Duration.ofMinutes(3);
        await().pollInterval(pollInterval).atMost(timeout).untilAsserted(() -> {
            var state = consumerClient.getTransferProcessState(transferProcessId);
            assertThat(TransferProcessStates.valueOf(state).code()).isGreaterThanOrEqualTo(TransferProcessStates.COMPLETED.code());
        });

        var bigQuery = BigQueryOptions.newBuilder()
                .setProjectId("edc-test-project")
                .setHost(BQ_CONTAINER.getEmulatorHttpEndpoint())
                .setLocation(BQ_CONTAINER.getEmulatorHttpEndpoint())
                .setCredentials(NoCredentials.getInstance())
                .build().getService();

        try {
            var queryConfigBuilder = QueryJobConfiguration.newBuilder("SELECT * from edc-test-dataset.table_dst;");
            var queryConfig = queryConfigBuilder
                    .setUseLegacySql(false)
                    .setUseQueryCache(true)
                    .setDestinationTable(TableId.of("edc-test-project", "edc-test-dataset", "table_temp_2"))
                    .build();

            var jobId = JobId.of(UUID.randomUUID().toString());
            var jobInfo = JobInfo.newBuilder(queryConfig).setJobId(jobId).build();
            final var queryJob = bigQuery.create(jobInfo);
            var results = queryJob.getQueryResults();

            assertThat(TEST_ROWS).hasSameElementsAs(results.getValues());
        } catch (InterruptedException interruptedException) {
            fail("Interrupted exception while waiting for results");
        }
    }

    @Test
    void transferTable_sinkFail() {
        assertTrue(BQ_CONTAINER.isRunning());

        var ports = BQ_CONTAINER.getExposedPorts();
        System.setProperty("EDC_GCP_BQ_REST", "http://localhost:" + BQ_CONTAINER.getMappedPort(ports.get(0)));
        System.setProperty("EDC_GCP_BQ_RPC", "http://localhost:" + BQ_CONTAINER.getMappedPort(ports.get(1)));

        System.setProperty("edc.gcp.bq.rest", "http://localhost:" + BQ_CONTAINER.getMappedPort(ports.get(0)));
        System.setProperty("edc.gcp.bq.rpc", "http://localhost:" + BQ_CONTAINER.getMappedPort(ports.get(1)));

        var assetId = providerClient.createBigQueryAsset(
                "Test asset",
                "edc-test-project",
                "edc-test-dataset",
                "sample_model",
                "table_temp_wrong",
                """
#standardSQL
SELECT * from edc-test-dataset.table_src_wrong;
                """);

        var policyId = providerClient.createPolicyDefinition(PolicyFixtures.noConstraintPolicy());
        providerClient.createContractDefinition(assetId, UUID.randomUUID().toString(), policyId, policyId);
        var transferProcessId = consumerClient.requestAssetAndTransferToBigQuery(providerClient, assetId,
                "edc-test-project", "edc-test-dataset", "table_dst");

        Duration pollInterval = Duration.ofSeconds(4);
        Duration timeout = Duration.ofMinutes(2);
        await().pollInterval(pollInterval).atMost(timeout).untilAsserted(() -> {
            var state = consumerClient.getTransferProcessState(transferProcessId);
            assertThat(TransferProcessStates.valueOf(state).code()).isGreaterThanOrEqualTo(TransferProcessStates.TERMINATED.code());
        });
    }

    @Test
    void transferTable_sourceFail() {
        assertTrue(BQ_CONTAINER.isRunning());

        var ports = BQ_CONTAINER.getExposedPorts();
        System.setProperty("EDC_GCP_BQ_REST", "http://localhost:" + BQ_CONTAINER.getMappedPort(ports.get(0)));
        System.setProperty("EDC_GCP_BQ_RPC", "http://localhost:" + BQ_CONTAINER.getMappedPort(ports.get(1)));

        System.setProperty("edc.gcp.bq.rest", "http://localhost:" + BQ_CONTAINER.getMappedPort(ports.get(0)));
        System.setProperty("edc.gcp.bq.rpc", "http://localhost:" + BQ_CONTAINER.getMappedPort(ports.get(1)));

        var assetId = providerClient.createBigQueryAsset(
                "Test asset",
                "edc-test-project",
                "edc-test-dataset",
                "sample_model",
                "table_temp_1",
                """
#standardSQL
SELECT * from edc-test-dataset.table_src_wrong;
                """);

        var policyId = providerClient.createPolicyDefinition(PolicyFixtures.noConstraintPolicy());
        providerClient.createContractDefinition(assetId, UUID.randomUUID().toString(), policyId, policyId);
        var transferProcessId = consumerClient.requestAssetAndTransferToBigQuery(providerClient, assetId,
                "edc-test-project", "edc-test-dataset", "table_dst");

        Duration pollInterval = Duration.ofSeconds(4);
        Duration timeout = Duration.ofMinutes(2);
        await().pollInterval(pollInterval).atMost(timeout).untilAsserted(() -> {
            var state = consumerClient.getTransferProcessState(transferProcessId);
            assertThat(TransferProcessStates.valueOf(state).code()).isGreaterThanOrEqualTo(TransferProcessStates.TERMINATED.code());
        });
    }
}
