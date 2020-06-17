package io.pravega.connectors.flink.table.catalog.pravega.factories;

import io.pravega.connectors.flink.table.catalog.pravega.PravegaCatalog;
import io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator;
import org.apache.flink.table.catalog.Catalog;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.CatalogFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.*;
import static org.apache.flink.table.descriptors.CatalogDescriptorValidator.CATALOG_DEFAULT_DATABASE;

public class PravegaCatalogFactory implements CatalogFactory {
    @Override
    public Catalog createCatalog(String name, Map<String, String> properties) {
        DescriptorProperties dp = getValidateProperties(properties);
        String defaultDB = dp.getOptionalString(CATALOG_DEFAULT_DATABASE).orElse("default-scope");
        String controllerUri = dp.getString(CATALOG_CONTROLLER_URI);
        String schemaRegistryUri = dp.getString(CATALOG_SCHEMA_REGISTRY_URI);

        return new PravegaCatalog(controllerUri, schemaRegistryUri, name, dp.asMap(), defaultDB);
    }

    @Override
    public Map<String, String> requiredContext() {
        HashMap<String, String> context = new HashMap<>();
        context.put(CATALOG_TYPE, CATALOG_TYPE_VALUE_PRAVEGA);
        context.put(CATALOG_PROPERTY_VERSION, "1");
        return context;
    }

    @Override
    public List<String> supportedProperties() {
        List props = new ArrayList<String>();
        props.add(CATALOG_CONTROLLER_URI);
        props.add(CATALOG_SCHEMA_REGISTRY_URI);
        props.add(CATALOG_DEFAULT_DATABASE);
        props.add(CATALOG_PRAVEGA_VERSION);
        return props;
    }

    private DescriptorProperties getValidateProperties(Map<String, String> properties) {
        DescriptorProperties dp = new DescriptorProperties();
        dp.putProperties(properties);
        new PravegaCatalogValidator().validate(dp);
        return dp;
    }
}
