package io.pravega.connectors.flink.table.catalog.pravega.descriptors;

import org.apache.flink.table.descriptors.CatalogDescriptor;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.util.Map;

import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_TYPE_VALUE_PRAVEGA;
import static io.pravega.connectors.flink.table.catalog.pravega.descriptors.PravegaCatalogValidator.CATALOG_PRAVEGA_VERSION;


public class PravegaCatalogDescriptor extends CatalogDescriptor {
    private String pravegaVersion;

    public PravegaCatalogDescriptor() {
        super(CATALOG_TYPE_VALUE_PRAVEGA,1, "public/default");
    }

    public PravegaCatalogDescriptor pravegaVersion(String pravegaVersion) {
        Preconditions.checkArgument(!StringUtils.isNullOrWhitespaceOnly(pravegaVersion));
        this.pravegaVersion=pravegaVersion;
        return this;
    }

    @Override
    protected Map<String, String> toCatalogProperties() {
        DescriptorProperties props = new DescriptorProperties();

        if(pravegaVersion!=null) {
            props.putString(CATALOG_PRAVEGA_VERSION, pravegaVersion);
        }

        return props.asMap();
    }
}

