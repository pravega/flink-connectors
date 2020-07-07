package io.pravega.connectors.flink.table.descriptors;

import org.apache.flink.table.descriptors.AvroValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptor;

import java.util.Map;

public class PravegaRegistry extends FormatDescriptor {

    public PravegaRegistry() {
        super(PravegaRegistryValidator.FORMAT_TYPE_VALUE, 1);
    }

    @Override
    protected Map<String, String> toFormatProperties() {
        final DescriptorProperties properties = new DescriptorProperties();

        return properties.asMap();
    }
}
