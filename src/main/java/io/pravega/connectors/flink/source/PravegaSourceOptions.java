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

package io.pravega.connectors.flink.source;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.Properties;
import java.util.function.Function;

/** Configurations for PravegaSource. */
@Internal
public class PravegaSourceOptions {
    public static final ConfigOption<Long> READER_TIMEOUT_MS =
            ConfigOptions.key("reader.timeout.ms")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription("The max time to wait when closing components.");

    public static <T> T getOption(
            Properties props, ConfigOption configOption, Function<String, T> parser) {
        String value = props.getProperty(configOption.key());
        return (T) (value == null ? configOption.defaultValue() : parser.apply(value));
    }
}
