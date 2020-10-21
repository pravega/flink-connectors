package io.pravega.connectors.flink.dynamic.table;

public class FlinkPravegaBatchDynamicTableFactory extends FlinkPravegaDynamicTableFactoryBase {
    public static final String IDENTIFIER = "pravega-batch";

    @Override
    protected boolean isStreamEnvironment() {
        return false;
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }
}
