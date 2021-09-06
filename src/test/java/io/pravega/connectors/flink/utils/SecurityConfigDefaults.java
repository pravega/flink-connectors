/**
 * Copyright Pravega Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.pravega.connectors.flink.utils;

public class SecurityConfigDefaults {
    public static final String PRAVEGA_USERNAME = "admin";
    public static final String PRAVEGA_PASSWORD = "1111_aaaa";
    public static final String PASSWD_FILE = "passwd";
    public static final String KEY_FILE = "server-key.key";
    public static final String CERT_FILE = "server-cert.crt";
    public static final String CLIENT_TRUST_STORE_FILE = "ca-cert.crt";
    public static final String STANDALONE_KEYSTORE_FILE = "server.keystore.jks";
    public static final String STANDALONE_TRUSTSTORE_FILE = "client.truststore.jks";
    public static final String STANDALONE_KEYSTORE_PASSWD_FILE = "server.keystore.jks.passwd";
}
