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

package org.eclipse.edc.vault.gcp;

import com.google.api.gax.rpc.NotFoundException;
import com.google.api.gax.rpc.StatusCode;
import com.google.cloud.secretmanager.v1.ProjectName;
import com.google.cloud.secretmanager.v1.Replication;
import com.google.cloud.secretmanager.v1.Secret;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretName;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import org.eclipse.edc.spi.monitor.Monitor;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

import java.util.ArrayList;
import java.util.Random;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.TestInstance.Lifecycle;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@TestInstance(Lifecycle.PER_CLASS)
class GcpSecretManagerVaultTest {
    private static final String TEST_REGION = "europe-west3";
    private static final String TEST_PROJECT = "project";
    private static final String VALID_KEY = "test";
    private static final String INVALID_KEY = "test=";
    private static final String SANITIZED_KEY = "test-_06924DEB";

    private static final int MAX_KEY_LENGTH = 255;

    private Monitor monitor;
    private SecretManagerServiceClient secretClient;
    private GcpSecretManagerVault vault;
    private final Secret testSecret =
                    Secret.newBuilder()
                    .setReplication(
                        Replication.newBuilder()
                        .setUserManaged(Replication.UserManaged.newBuilder()
                            .addReplicas(Replication.UserManaged.Replica.newBuilder()
                                .setLocation(TEST_REGION)
                                .build())
                            .build())
                        .build())
                    .build();

    private final ArrayList<Character> validChars = new ArrayList<Character>();
    private final ArrayList<Character> invalidChars = new ArrayList<Character>();
    private final Random randGen = new Random();

    @BeforeAll
    void init() {
        // Init the array with the chars allowed and not allowed in a Secret Key.
        // Secret key is a string with a maximum length of 255 characters and can
        // contain uppercase and lowercase letters, digits, and the hyphen (`-`) and
        // underscore (`_`) characters.
        for (char c = 'a'; c <= 'z'; c++) {
            validChars.add(c);
        }
        for (char c = 'A'; c <= 'Z'; c++) {
            validChars.add(c);
        }
        for (char c = '0'; c <= '9'; c++) {
            validChars.add(c);
        }
        validChars.add('-');
        validChars.add('_');

        for (char c = 32; c <= 126; c++) {
            if (!Character.isLetterOrDigit(c) && c != '_' && c != '-') {
                invalidChars.add(c);
            }
        }

        randGen.setSeed(System.currentTimeMillis());
    }

    @BeforeEach
    void instantiateMocksAndVault() {
        monitor = mock();
        secretClient = mock();
        vault = new GcpSecretManagerVault(monitor, TEST_PROJECT, TEST_REGION, secretClient);
    }

    @Test
    void storeSecret_shallSanitizeKey() {
        var parent = ProjectName.of(TEST_PROJECT);
        when(secretClient.createSecret(parent, SANITIZED_KEY, testSecret))
                .thenReturn(testSecret);

        var randomContent = UUID.randomUUID().toString();
        vault.storeSecret(INVALID_KEY, randomContent);
        verify(secretClient).createSecret(parent, SANITIZED_KEY, testSecret);
    }

    @Test
    void resolveSecret_shallSanitizeKey() {
        vault.resolveSecret(INVALID_KEY);

        var secretVersionName = SecretVersionName.of(TEST_PROJECT, SANITIZED_KEY, "latest");
        verify(secretClient).accessSecretVersion(secretVersionName);
    }

    @Test
    void deleteSecret_shallSanitizeKey() {
        vault.deleteSecret(INVALID_KEY);

        verify(secretClient).deleteSecret(SecretName.of(TEST_PROJECT, SANITIZED_KEY));
    }

    @Test
    void sanitizeKey_testValidKeys() {
        for (int i = 0; i < 100; i++) {
            int len = 1 + randGen.nextInt(MAX_KEY_LENGTH);
            var validKey = getRandomKeyWithValidChars(len);
            var sanitizedKey = vault.sanitizeKey(validKey);
            assertThat(validKey.equals(sanitizedKey)).isTrue();
            assertThat(isValidKey(sanitizedKey)).isTrue();
        }
    }

    @Test
    void sanitizeKey_testLongKeyWithValidCharKey() {
        for (int i = 0; i < 100; i++) {
            int len = MAX_KEY_LENGTH + 1 + randGen.nextInt(MAX_KEY_LENGTH);
            var longKey = getRandomKeyWithValidChars(len);
            var sanitizedKey = vault.sanitizeKey(longKey);
            assertThat(isValidKey(longKey)).isFalse();
            assertThat(isValidKey(sanitizedKey)).isTrue();
        }
    }

    @Test
    void sanitizeKey_testLongKeyWithInvalidCharKey() {
        for (int i = 0; i < 100; i++) {
            int len = MAX_KEY_LENGTH + 1 + randGen.nextInt(MAX_KEY_LENGTH);
            int invCharCount = 1 + randGen.nextInt(len);
            var longInvalidKey = getRandomKeyWithInvalidChars(len, invCharCount);
            var sanitizedKey = vault.sanitizeKey(longInvalidKey);
            assertThat(isValidKey(longInvalidKey)).isFalse();
            assertThat(isValidKey(sanitizedKey)).isTrue();
        }
    }

    @Test
    void sanitizeKey_longKeysDifferingAfter255CharsStillDifferent() {
        for (int i = 0; i < 100; i++) {
            int len = MAX_KEY_LENGTH + 1 + randGen.nextInt(MAX_KEY_LENGTH);
            var longKey = getRandomKeyWithValidChars(len);
            int extraLen = len - MAX_KEY_LENGTH;
            int diffPosition = MAX_KEY_LENGTH + randGen.nextInt(extraLen);
            var sb = new StringBuilder(longKey);
            char c = sb.charAt(diffPosition);
            do {
                c = (char) (c + 1);
            } while (!validChars.contains(c));

            sb.setCharAt(diffPosition, (char) (c + 1));
            var longDiffKey = sb.toString();

            var sanitizedKey = vault.sanitizeKey(longKey);
            var sanitizedDiffKey = vault.sanitizeKey(longDiffKey);
            assertThat(sanitizedKey.equals(sanitizedDiffKey)).isFalse();
            assertThat(isValidKey(sanitizedKey)).isTrue();
            assertThat(isValidKey(sanitizedDiffKey)).isTrue();
        }
    }

    @Test
    void resolveSecret_shallReturnNullAndLogDebugIfSecretNotFound() {
        var status = new TestStatusCode();
        when(secretClient.accessSecretVersion(isA(SecretVersionName.class)))
                .thenThrow(new NotFoundException("test", new Exception("test"), status, false));
        var result = vault.resolveSecret(VALID_KEY);

        assertThat(result).isNull();
        verify(monitor).debug(anyString(), isA(NotFoundException.class));
    }

    @Test
    void resolveSecret_shallReturnNullAndLogSevereOnGenericException() {
        when(secretClient.accessSecretVersion(isA(SecretVersionName.class)))
                .thenThrow(new RuntimeException("test"));

        var result = vault.resolveSecret(VALID_KEY);

        assertThat(result).isNull();
        verify(monitor).severe(anyString(), isA(RuntimeException.class));
    }

    private boolean isValidKey(String key) {
        if (key.length() > MAX_KEY_LENGTH) {
            return false;
        }

        for (int i = 0; i < key.length(); i++) {
            var c = key.charAt(i);
            if (!Character.isLetterOrDigit(c) && c != '_' && c != '-') {
                return false;
            }
        }

        return true;
    }

    private char getRandomValidChar() {
        return validChars.get(randGen.nextInt(validChars.size()));
    }

    private char getRandomInvalidChar() {
        return invalidChars.get(randGen.nextInt(invalidChars.size()));
    }

    private String getRandomKeyWithValidChars(int length) {
        var sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            var c = getRandomValidChar();
            sb.append(c);
        }
        return sb.toString();
    }

    private String getRandomKeyWithInvalidChars(int length, int errors) {
        var key = getRandomKeyWithValidChars(length);
        var keyBuilder = new StringBuilder(key);
        ArrayList<Integer> positions = new ArrayList<Integer>();
        for (int i = 0; i < length; i++) {
            positions.add(i);
        }

        for (int i = 0; i < errors && positions.size() > 0; i++) {
            int index = randGen.nextInt(positions.size());
            int position = positions.get(index);
            positions.remove(index);

            keyBuilder.setCharAt(position, getRandomInvalidChar());
        }

        return keyBuilder.toString();
    }

    private class TestStatusCode implements StatusCode {
        @Override
        public Integer getTransportCode() {
            return Integer.valueOf(0);
        }

        @Override
        public Code getCode() {
            return Code.OK;
        }
    }
}

