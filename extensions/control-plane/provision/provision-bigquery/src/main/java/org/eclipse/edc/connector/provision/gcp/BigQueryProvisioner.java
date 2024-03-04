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

package org.eclipse.edc.connector.provision.gcp;

import org.eclipse.edc.connector.transfer.spi.provision.Provisioner;
import org.eclipse.edc.connector.transfer.spi.types.DeprovisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionResponse;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.ResourceDefinition;
import org.eclipse.edc.gcp.bigquery.BigQueryService;
import org.eclipse.edc.gcp.bigquery.BigQueryServiceImpl;
import org.eclipse.edc.gcp.bigquery.BigQueryTarget;
import org.eclipse.edc.gcp.common.GcpAccessToken;
import org.eclipse.edc.gcp.common.GcpConfiguration;
import org.eclipse.edc.gcp.common.GcpException;
import org.eclipse.edc.gcp.common.GcpServiceAccount;
import org.eclipse.edc.gcp.iam.IamService;
import org.eclipse.edc.gcp.iam.IamServiceImpl;
import org.eclipse.edc.policy.model.Policy;
import org.eclipse.edc.spi.monitor.Monitor;
import org.eclipse.edc.spi.response.ResponseStatus;
import org.eclipse.edc.spi.response.StatusResult;
import org.eclipse.edc.spi.types.TypeManager;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static java.util.concurrent.CompletableFuture.completedFuture;

public class BigQueryProvisioner implements Provisioner<BigQueryResourceDefinition, BigQueryProvisionedResource> {
    private final GcpConfiguration gcpConfiguration;
    private final Monitor monitor;
    private final IamService iamService;
    private final TypeManager typeManager;
    private BigQueryService bigQueryService;

    private BigQueryProvisioner(GcpConfiguration gcpConfiguration, Monitor monitor, IamService iamService, TypeManager typeManager) {
        this.monitor = monitor;
        this.iamService = iamService;
        this.gcpConfiguration = gcpConfiguration;
        this.typeManager = typeManager;
    }

    @Override
    public boolean canProvision(ResourceDefinition resourceDefinition) {
        return resourceDefinition instanceof BigQueryResourceDefinition;
    }

    @Override
    public boolean canDeprovision(ProvisionedResource resourceDefinition) {
        return resourceDefinition instanceof BigQueryProvisionedResource;
    }

    @Override
    public CompletableFuture<StatusResult<ProvisionResponse>> provision(
            BigQueryResourceDefinition resourceDefinition, Policy policy) {
        var target = getTarget(resourceDefinition);
        monitor.info("BigQuery Provisioner provision " + target.getTableName());
        var bqIamService = getIamService(resourceDefinition);
        var serviceAccountName = getServiceAccountName(resourceDefinition);

        var tableName = Optional.ofNullable(target.table())
                .orElseGet(() -> {
                    var generatedTableName = resourceDefinition.getId();
                    monitor.debug("BigQuery Provisioner table name generated: " + generatedTableName);
                    return generatedTableName;
                });
        var resourceName = tableName + "-table";

        // TODO update target with the generated table name.

        try {
            String serviceAccountEmail = null;
            GcpServiceAccount serviceAccount = null;
            if (serviceAccountName != null) {
                serviceAccount = bqIamService.getServiceAccount(serviceAccountName);
                serviceAccountEmail = serviceAccount.getEmail();
            }

            if (bigQueryService == null) {
                bigQueryService = BigQueryServiceImpl.Builder.newInstance(gcpConfiguration, target, monitor)
                        .serviceAccount(serviceAccountEmail)
                        .build();
            }

            if (!bigQueryService.tableExists(target)) {
                monitor.warning("BigQuery Provisioner table " + target.getTableName() + " DOESN'T exist");
                return completedFuture(StatusResult.failure(ResponseStatus.FATAL_ERROR, "Table " + target.getTableName().toString() + " doesn't exist"));
            }
            monitor.info("BigQuery Provisioner table " + target.getTableName().toString() + " exists");

            GcpAccessToken token = null;

            if (serviceAccount != null) {
                token = bqIamService.createAccessToken(serviceAccount);
            } else {
                serviceAccount = new GcpServiceAccount("adc-email", "adc-name", "application default");
                token = bqIamService.createDefaultAccessToken();
            }
            monitor.info("BigQuery Provisioner token ready");

            var resource = getProvisionedResource(resourceDefinition, resourceName, tableName, serviceAccount);
            var response = ProvisionResponse.Builder.newInstance().resource(resource).secretToken(token).build();
            return CompletableFuture.completedFuture(StatusResult.success(response));
        } catch (GcpException gcpException) {
            return completedFuture(StatusResult.failure(ResponseStatus.FATAL_ERROR, gcpException.toString()));
        }
    }

    private BigQueryProvisionedResource getProvisionedResource(BigQueryResourceDefinition resourceDefinition, String resourceName, String table, GcpServiceAccount serviceAccount) {
        String serviceAccountName = null;
        if (serviceAccount != null) {
            serviceAccountName = serviceAccount.getName();
        }

        return BigQueryProvisionedResource.Builder.newInstance()
            .properties(resourceDefinition.getProperties())
            .id(resourceDefinition.getId())
            .resourceDefinitionId(resourceDefinition.getId())
            .transferProcessId(resourceDefinition.getTransferProcessId())
            .resourceName(resourceName)
            .project(resourceDefinition.getProject())
            .dataset(resourceDefinition.getDataset())
            .table(table)
            .serviceAccountName(serviceAccountName)
            .hasToken(true).build();
    }

    @Override
    public CompletableFuture<StatusResult<DeprovisionedResource>> deprovision(
            BigQueryProvisionedResource provisionedResource, Policy policy) {
        return CompletableFuture.completedFuture(StatusResult.success(
            DeprovisionedResource.Builder.newInstance()
                .provisionedResourceId(provisionedResource.getId()).build()));
    }

    private String getServiceAccountName(BigQueryResourceDefinition resourceDefinition) {
        if (resourceDefinition.getServiceAccountName() != null) {
            // TODO verify service account name from resource definition before returning.
            return resourceDefinition.getServiceAccountName();
        }

        return gcpConfiguration.getServiceAccountName();
    }

    private IamService getIamService(BigQueryResourceDefinition resourceDefinition) {
        var target = getTarget(resourceDefinition);
        // TODO verify the credentials for IAM access.
        return IamServiceImpl.Builder.newInstance(monitor, target.project())
            .build();
    }

    private BigQueryTarget getTarget(BigQueryResourceDefinition resourceDefinition) {
        var project = resourceDefinition.getProject();
        var dataset = resourceDefinition.getDataset();
        var table = resourceDefinition.getTable();

        if (project == null) {
            project = gcpConfiguration.getProjectId();
        }

        return new BigQueryTarget(project, dataset, table);
    }

    public static class Builder {
        private final BigQueryProvisioner bqProvisioner;

        public static Builder newInstance(GcpConfiguration gcpConfiguration, Monitor monitor, IamService iamService, TypeManager typeManager) {
            return new Builder(gcpConfiguration, monitor, iamService, typeManager);
        }

        private Builder(GcpConfiguration gcpConfiguration, Monitor monitor, IamService iamService, TypeManager typeManager) {
            bqProvisioner = new BigQueryProvisioner(gcpConfiguration, monitor, iamService, typeManager);
        }

        public Builder bigQueryService(BigQueryService bigQueryService) {
            bqProvisioner.bigQueryService = bigQueryService;
            return this;
        }

        public BigQueryProvisioner build() {
            return bqProvisioner;
        }
    }
}
