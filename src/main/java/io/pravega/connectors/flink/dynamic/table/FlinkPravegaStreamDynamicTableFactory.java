package io.pravega.connectors.flink.dynamic.table;

public class FlinkPravegaStreamDynamicTableFactory extends FlinkPravegaDynamicTableFactoryBase{

    public static final String IDENTIFIER = "pravega-stream";

    @Override
    protected boolean isStreamEnvironment() {
        return true;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }
}
