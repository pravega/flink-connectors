package io.pravega.connectors.flink.table.catalog.pravega.descriptors;

import org.apache.flink.table.descriptors.CatalogDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_CONTROLLER_URI;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_SCHEMA_REGISTRY_URI;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_TYPE_VALUE_PRAVEGA;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;

public class PravegaCatalogDescriptor extends CatalogDescriptor {

    private final String controllerUri;
    private final String schemaRegistryUri;

    public PravegaCatalogDescriptor(String controllerUri, String schemaRegistryUri, String defaultDatabase) {
        super(CATALOG_TYPE_VALUE_PRAVEGA,1, defaultDatabase);

        this.controllerUri = controllerUri;
        this.schemaRegistryUri = schemaRegistryUri;
    }

    @Override
    protected Map<String, String> toCatalogProperties() {
        final DescriptorProperties properties = new DescriptorProperties();

        properties.putString(CATALOG_CONTROLLER_URI, controllerUri);
        properties.putString(CATALOG_SCHEMA_REGISTRY_URI, schemaRegistryUri);

        return properties.asMap();
    }
}

