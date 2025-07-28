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

import com.fasterxml.jackson.annotation.JsonCreator;
import io.restassured.http.ContentType;
import org.eclipse.edc.connector.controlplane.test.system.utils.LazySupplier;
import org.eclipse.edc.connector.controlplane.test.system.utils.Participant;
import org.eclipse.edc.gcp.bigquery.service.BigQueryServiceSchema;
import org.eclipse.edc.spi.system.configuration.Config;
import org.eclipse.edc.spi.system.configuration.ConfigFactory;

import java.net.URI;
import java.util.Map;
import java.util.UUID;

import static jakarta.json.Json.createObjectBuilder;
import static org.eclipse.edc.util.io.Ports.getFreePort;

public class BigQueryTransferParticipant extends Participant {

    private final LazySupplier<URI> publicEndpoint = new LazySupplier<>(() -> URI.create("http://localhost:%d%s".formatted(getFreePort(), "/public")));
    private final LazySupplier<URI> controlEndpoint = new LazySupplier<>(() -> URI.create("http://localhost:%d%s".formatted(getFreePort(), "/control")));

    public Config getConfig() {
        return ConfigFactory.fromMap(Map.ofEntries(
                Map.entry("edc.participant.id", id),
                Map.entry("edc.dsp.callback.address", controlPlaneProtocol.get().toString()),
                Map.entry("web.http.port", String.valueOf(getFreePort())),
                Map.entry("web.http.path", "/api"),
                Map.entry("web.http.management.port", String.valueOf(controlPlaneManagement.get().getPort())),
                Map.entry("web.http.management.path", controlPlaneManagement.get().getPath()),
                Map.entry("web.http.protocol.port", String.valueOf(controlPlaneProtocol.get().getPort())),
                Map.entry("web.http.protocol.path", controlPlaneProtocol.get().getPath()),
                Map.entry("web.http.public.port", String.valueOf(publicEndpoint.get().getPort())),
                Map.entry("web.http.public.path", publicEndpoint.get().getPath()),
                Map.entry("web.http.control.port", String.valueOf(controlEndpoint.get().getPort())),
                Map.entry("web.http.control.path", controlEndpoint.get().getPath()),
                Map.entry("edc.public.key.alias", "public-key"),
                Map.entry("edc.transfer.dataplane.token.signer.privatekey.alias", "1"),
                Map.entry("edc.transfer.proxy.token.signer.privatekey.alias", "1"),
                Map.entry("edc.transfer.proxy.token.verifier.publickey.alias", "public-key"),
                Map.entry("edc.dataplane.token.validation.endpoint", "http://localhost:19192/control/token"),
                Map.entry("edc.gcp.project.id", "edc-test-project")
        ));
    }

    public String createBigQueryAsset(String name, String project, String dataset, String table, String destinationTable, String query) {
        var assetId = UUID.randomUUID().toString();

        Map<String, Object> dataAddressProperties = Map.of(
                "type", BigQueryServiceSchema.BIGQUERY_DATA,
                "name", name,

                BigQueryServiceSchema.PROJECT, project,
                BigQueryServiceSchema.DATASET, dataset,
                BigQueryServiceSchema.TABLE, table,
                BigQueryServiceSchema.DESTINATION_TABLE, destinationTable,
                BigQueryServiceSchema.QUERY, query
        );

        Map<String, Object> properties = Map.of(
                "name", assetId,
                "contenttype", "application/json",
                "version", "1.0"
        );

        return createAsset(assetId, properties, dataAddressProperties);
    }

    public Map<String, Object> getDataDestination(String transferProcessId) {
        return baseManagementRequest()
                .contentType(ContentType.JSON)
                .when()
                .get("/v2/transferprocesses/{id}", transferProcessId)
                .then()
                .statusCode(200)
                .extract().jsonPath().get("'dataDestination'");
    }

    public String requestAssetAndTransferToBigQuery(Participant provider, String assetId, String project, String dataset, String table) {
        var destination = createObjectBuilder()
                .add("type", BigQueryServiceSchema.BIGQUERY_DATA)
                .add("properties", createObjectBuilder()
                .add(BigQueryServiceSchema.PROJECT, project)
                .add(BigQueryServiceSchema.DATASET, dataset)
                .add(BigQueryServiceSchema.TABLE, table))
                .build();
        return this.requestAssetFrom(assetId, provider)
                .withTransferType("BigQueryData-PUSH")
                .withDestination(destination)
                .execute();
    }

    public static final class Builder extends Participant.Builder<BigQueryTransferParticipant, Builder> {

        private Builder() {
            super(new BigQueryTransferParticipant());
        }

        @JsonCreator
        public static Builder newInstance() {
            return new Builder();
        }

        @Override
        public BigQueryTransferParticipant build() {
            super.build();
            return participant;
        }
    }
}

