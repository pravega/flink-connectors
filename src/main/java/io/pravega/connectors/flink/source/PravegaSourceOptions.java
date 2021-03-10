package io.pravega.connectors.flink.source;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

import java.util.Properties;
import java.util.function.Function;

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
