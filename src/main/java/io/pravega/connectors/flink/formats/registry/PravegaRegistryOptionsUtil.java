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

package io.pravega.connectors.flink.formats.registry;

import io.pravega.connectors.flink.PravegaConfig;
import io.pravega.connectors.flink.util.FlinkPravegaUtils;
import org.apache.flink.configuration.ReadableConfig;

import java.util.Optional;

import static io.pravega.connectors.flink.util.FlinkPravegaUtils.isCredentialsLoadDynamic;

public class PravegaRegistryOptionsUtil {

    // --------------------------------------------------------------------------------------------
    // Utilities
    // --------------------------------------------------------------------------------------------

    public static PravegaConfig getPravegaConfig(ReadableConfig tableOptions) {
        PravegaConfig pravegaConfig = PravegaConfig.fromDefaults()
                .withDefaultScope(tableOptions.get(PravegaRegistryOptions.NAMESPACE))
                .withHostnameValidation(tableOptions.get(PravegaRegistryOptions.SECURITY_VALIDATE_HOSTNAME))
                .withTrustStore(tableOptions.get(PravegaRegistryOptions.SECURITY_TRUST_STORE));

        Optional<String> authType = tableOptions.getOptional(PravegaRegistryOptions.SECURITY_AUTH_TYPE);
        Optional<String> authToken = tableOptions.getOptional(PravegaRegistryOptions.SECURITY_AUTH_TOKEN);
        if (authType.isPresent() && authToken.isPresent() && !isCredentialsLoadDynamic()) {
            pravegaConfig.withCredentials(new FlinkPravegaUtils.SimpleCredentials(authType.get(), authToken.get()));
        }

        return pravegaConfig;
    }
}
