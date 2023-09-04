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
 *       Google LCC - Initial implementation
 *
 */

package org.eclipse.edc.connector.provision.gcp;

import com.google.cloud.storage.Blob;
import com.google.cloud.storage.StorageOptions;
import org.eclipse.edc.connector.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.StatusChecker;
import org.eclipse.edc.connector.transfer.spi.types.TransferProcess;
import org.eclipse.edc.gcp.common.GcpCredentials;
import org.eclipse.edc.gcp.common.GcpServiceAccountCredentials;
import org.eclipse.edc.gcp.storage.StorageServiceImpl;
import org.eclipse.edc.spi.EdcException;
import org.eclipse.edc.spi.monitor.Monitor;
import org.jetbrains.annotations.Nullable;

import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;
import static org.eclipse.edc.connector.transfer.spi.types.TransferProcess.Type.PROVIDER;

public class GcsProvisionerStatusChecker implements StatusChecker {

    private final Monitor monitor;
    private final GcpCredentials gcpCredential;

    private final @Nullable String projectId;

    public GcsProvisionerStatusChecker(Monitor monitor, GcpCredentials gcpCredential, @Nullable String projectId) {
        this.monitor = monitor;
        this.gcpCredential = gcpCredential;
        this.projectId = projectId;
    }

    @Override
    public boolean isComplete(TransferProcess transferProcess, List<ProvisionedResource> resources) {
        if (transferProcess.getType() == PROVIDER) {
            // TODO check if PROVIDER process implementation is needed
        }
        var gcsResourceDefinition = (GcsResourceDefinition) new GcsConsumerResourceDefinitionGenerator().generate(transferProcess.getDataRequest(), null);

        if (resources != null && !resources.isEmpty()) {
            for (var resource : resources) {
                if (resource instanceof GcsProvisionedResource) {
                    return checkBucketTransferComplete(gcsResourceDefinition);
                }
            }
        } else {
            return checkBucketTransferComplete(gcsResourceDefinition);
        }
        throw new EdcException(format("Cannot determine completion: no resource associated with transfer process %s.", transferProcess));
    }

    private boolean checkBucketTransferComplete(GcsResourceDefinition gcsResourceDefinition) {
        var bucketName = gcsResourceDefinition.getBucketName();
        var testBlobName = bucketName + ".complete";

        var tokenKeyName = gcsResourceDefinition.getTokenKeyName();
        var serviceAccountKeyName = gcsResourceDefinition.getServiceAccountKeyName();
        var serviceAccountKeyValue = gcsResourceDefinition.getServiceAccountKeyValue();
        var gcpServiceAccountCredentials = new GcpServiceAccountCredentials(tokenKeyName, serviceAccountKeyName, serviceAccountKeyValue);
        var googleCredentials = gcpCredential.resolveGoogleCredentialsFromDataAddress(gcpServiceAccountCredentials);

        var storageClient = StorageOptions.newBuilder()
                .setCredentials(googleCredentials)
                .setProjectId(projectId).build().getService();
        var storageService = new StorageServiceImpl(storageClient, monitor);

        var blobs = storageService.list(bucketName);
        // TODO rewrite with stream
        Iterator<Blob> blobIterator = blobs.iterateAll().iterator();
        while (blobIterator.hasNext()) {
            Blob blob = blobIterator.next();
            if (blob.getName().equals(testBlobName)) {
                return true;
            }
        }
        return false;
    }
}
