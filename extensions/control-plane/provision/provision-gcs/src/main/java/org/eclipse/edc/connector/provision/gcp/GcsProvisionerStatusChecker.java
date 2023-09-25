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

package org.eclipse.edc.connector.provision.gcp;

import org.eclipse.edc.connector.transfer.spi.types.ProvisionedResource;
import org.eclipse.edc.connector.transfer.spi.types.StatusChecker;
import org.eclipse.edc.connector.transfer.spi.types.TransferProcess;
import org.eclipse.edc.gcp.storage.GcsStoreSchema;
import org.eclipse.edc.gcp.storage.StorageService;

import java.util.List;

public class GcsProvisionerStatusChecker implements StatusChecker {
    private StorageService storageService;

    public GcsProvisionerStatusChecker(StorageService storageService) {
        this.storageService = storageService;
    }

    @Override
    public boolean isComplete(TransferProcess transferProcess, List<ProvisionedResource> resources) {
        if (resources != null && !resources.isEmpty()) {
            return resources.stream().allMatch(resource -> isResourceTransferCompleted(resource));
        } else {
            var bucketName = transferProcess.getDataRequest().getDataDestination().getStringProperty(GcsStoreSchema.BUCKET_NAME);
            return isBucketTransferCompleted(bucketName);
        }
    }

    private boolean isResourceTransferCompleted(ProvisionedResource resource) {
        if (!(resource instanceof GcsProvisionedResource)) {
            // Only handle GCS resources.
            return true;
        }
        return isBucketTransferCompleted(((GcsProvisionedResource) resource).getBucketName());
    }

    private boolean isBucketTransferCompleted(String bucketName) {
        var testBlobName = bucketName + ".complete";
        var blobs = storageService.list(bucketName);
        return blobs.streamAll().anyMatch(blob -> blob.getName().equals(testBlobName));
    }
}
