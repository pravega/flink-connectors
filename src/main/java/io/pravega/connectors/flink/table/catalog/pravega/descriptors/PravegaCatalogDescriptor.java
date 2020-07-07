package io.pravega.connectors.flink.table.catalog.pravega.descriptors;

import org.apache.flink.table.descriptors.CatalogDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;

import java.util.Map;

import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_TYPE_VALUE_PRAVEGA;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;

public class PravegaCatalogDescriptor extends CatalogDescriptor {

    private final String defaultDatabase;

    public PravegaCatalogDescriptor() {
        this("default-scope");
    }

    public PravegaCatalogDescriptor(String defaultDatabase) {
        super(CATALOG_TYPE_VALUE_PRAVEGA,1, defaultDatabase);
        this.defaultDatabase = defaultDatabase;
    }

    @Override
    protected Map<String, String> toCatalogProperties() {
        DescriptorProperties props = new DescriptorProperties();
        props.putString(CATALOG_DEFAULT_DATABASE, defaultDatabase);

        return props.asMap();
    }
}

