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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;

import java.time.Duration;
import java.util.Properties;
import java.util.function.Function;

public class PravegaSourceOptions {
    public static final String SOURCE_PREFIX = "pravega.source.";

    public static final ConfigOption<Long> READER_TIMEOUT_MS =
            ConfigOptions.key(SOURCE_PREFIX + "timeout.ms")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription("The max time to wait when closing components.");
    public static final ConfigOption<String> READER_GROUP_NAME =
            ConfigOptions.key(SOURCE_PREFIX + "readerGroupName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Configures the reader group name.");
    public static final ConfigOption<String> READER_GROUP_SCOPE =
            ConfigOptions.key(SOURCE_PREFIX + "readerGroupScope")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Configures the reader group scope for synchronization purposes.");
    public static final ConfigOption<Duration> READER_GROUP_REFRESH_TIME =
            ConfigOptions.key(SOURCE_PREFIX + "readerGroupRefreshTime")
                    .durationType()
                    .noDefaultValue()
                    .withDescription("Sets the group refresh time.");
    public static final ConfigOption<Duration> CHECKPOINT_INITIATE_TIMEOUT =
            ConfigOptions.key(SOURCE_PREFIX + "checkpointInitiateTimeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(5))
                    .withDescription("Sets the timeout for initiating a checkpoint in Pravega.");
    public static final ConfigOption<Duration> EVENT_READ_TIMEOUT =
            ConfigOptions.key(SOURCE_PREFIX + "eventReadTimeout")
                    .durationType()
                    .defaultValue(Duration.ofSeconds(1))
                    .withDescription(
                            Description.builder()
                                    .text("Sets the timeout for the call to read events from Pravega. After the timeout")
                                    .linebreak()
                                    .text("expires (without an event being returned), another call will be made.")
                                    .build());
    public static final ConfigOption<Integer> MAX_OUTSTANDING_CHECKPOINT_REQUEST =
            ConfigOptions.key(SOURCE_PREFIX + "maxOutstandingCheckpointRequest")
                    .intType()
                    .defaultValue(3)
                    .withDescription(
                            Description.builder()
                                    .text("Configures the maximum outstanding checkpoint requests to Pravega.")
                                    .linebreak()
                                    .text("Upon requesting more checkpoints than the specified maximum,")
                                    .linebreak()
                                    .text("(say a checkpoint request times out on the ReaderCheckpointHook but Pravega is still working on it),")
                                    .linebreak()
                                    .text("this configurations allows Pravega to limit any further checkpoint request being made to the ReaderGroup.")
                                    .linebreak()
                                    .text("This configuration is particularly relevant when multiple checkpoint requests need to be honored.")
                                    .linebreak()
                                    .text("(e.g., frequent savepoint requests being triggered concurrently)")
                                    .build());

    public static <T> T getOption(
            Properties props, ConfigOption configOption, Function<String, T> parser) {
        String value = props.getProperty(configOption.key());
        return (T) (value == null ? configOption.defaultValue() : parser.apply(value));
    }
}
