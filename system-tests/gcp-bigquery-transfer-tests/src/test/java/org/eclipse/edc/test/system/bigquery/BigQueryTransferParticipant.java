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
import org.eclipse.edc.connector.controlplane.test.system.utils.Participant;
import org.eclipse.edc.gcp.bigquery.service.BigQueryServiceSchema;

import java.util.Map;
import java.util.UUID;

import static io.restassured.RestAssured.given;
import static jakarta.json.Json.createObjectBuilder;

public class BigQueryTransferParticipant extends Participant {

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
        return given()
                .baseUri(managementEndpoint.getUrl().toString())
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

