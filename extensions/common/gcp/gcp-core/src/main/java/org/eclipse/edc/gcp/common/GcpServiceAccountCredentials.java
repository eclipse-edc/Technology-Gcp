/*
 *  Copyright (c) 2022 T-Systems International GmbH
 *
 *  This program and the accompanying materials are made available under the
 *  terms of the Apache License, Version 2.0 which is available at
 *  https://www.apache.org/licenses/LICENSE-2.0
 *
 *  SPDX-License-Identifier: Apache-2.0
 *
 *  Contributors:
 *       T-Systems International GmbH
 *
 */

package org.eclipse.edc.gcp.common;

public class GcpServiceAccountCredentials {
    private String vaultTokenKeyName;
    private String vaultServiceAccountKeyName;
    private String serviceAccountValue;

    /**
     * @param vaultTokenKeyName Key name of an entry in the vault containing an access token
     * @param vaultServiceAccountKeyName key name of an entry in the vault containing a valid Google Credentials file in json format
     * @param serviceAccountValue Content of a valid Google Credentials file in json format encoded with base64
     */
    public GcpServiceAccountCredentials(String vaultTokenKeyName, String vaultServiceAccountKeyName, String serviceAccountValue) {
        this.vaultTokenKeyName = vaultTokenKeyName;
        this.vaultServiceAccountKeyName = vaultServiceAccountKeyName;
        this.serviceAccountValue = serviceAccountValue;
    }

    public String getVaultTokenKeyName() { return vaultTokenKeyName; }

    public String getVaultServiceAccountKeyName() { return vaultServiceAccountKeyName; }

    public String getServiceAccountValue() { return serviceAccountValue; }
}
