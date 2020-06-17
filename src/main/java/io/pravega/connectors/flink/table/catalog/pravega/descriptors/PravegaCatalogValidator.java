package io.pravega.connectors.flink.table.catalog.pravega.descriptors;

import org.apache.flink.table.descriptors.CatalogDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

public class PravegaCatalogValidator extends CatalogDescriptorValidator {
    public static final String CATALOG_TYPE_VALUE_PRAVEGA = "pravega";
    public static final String CATALOG_PRAVEGA_VERSION = "pravega-version";
    public static final String CATALOG_CONTROLLER_URI = "controller-uri";

    @Override
    public void validate(DescriptorProperties properties) {
        super.validate(properties);
        properties.validateString(CATALOG_CONTROLLER_URI, false, 1);
    }
}
