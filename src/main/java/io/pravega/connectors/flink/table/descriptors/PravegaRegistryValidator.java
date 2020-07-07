package io.pravega.connectors.flink.table.descriptors;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;

import java.util.Arrays;

public class PravegaRegistryValidator extends FormatDescriptorValidator {

    public static final String FORMAT_TYPE_VALUE = "pravega-registry";

    public static final String FORMAT_SCHEMA_REGISTRY_URL = "format.schema-registry.url";
    public static final String FORMAT_SCHEMA_REGISTRY_NAMESPACE = "format.schema-registry.namespace";
    public static final String FORMAT_SCHEMA_REGISTRY_GROUP = "format.schema-registry.group";

    // Required for Serializer
    public static final String FORMAT_SCHEMA_REGISTRY_FORMAT = "format.schema-registry.format";
    public static final String FORMAT_SCHEMA_REGISTRY_SCHEMA = "format.schema-registry.schema";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);

        properties.validateString(FORMAT_SCHEMA_REGISTRY_URL, false, 1);
        properties.validateString(FORMAT_SCHEMA_REGISTRY_NAMESPACE, false, 1);
        properties.validateString(FORMAT_SCHEMA_REGISTRY_GROUP, false, 1);

        properties.validateEnumValues(FORMAT_SCHEMA_REGISTRY_FORMAT, true,
                Arrays.asList("json", "avro", "protobuf"));
        properties.validateString(FORMAT_SCHEMA_REGISTRY_SCHEMA, true, 1);
    }
}
