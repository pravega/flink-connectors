package io.pravega.connectors.flink.utils;

import org.apache.flink.table.client.config.Environment;
import org.apache.flink.util.FileUtils;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.Objects;

public class EnvironmentFileUtil {
    private EnvironmentFileUtil() {
        // private
    }

    public static Environment parseUnmodified(String fileName) throws IOException {
        final URL url = EnvironmentFileUtil.class.getClassLoader().getResource(fileName);
        Objects.requireNonNull(url);
        return Environment.parse(url);
    }

    public static Environment parseModified(String fileName, Map<String, String> replaceVars) throws IOException {
        final URL url = EnvironmentFileUtil.class.getClassLoader().getResource(fileName);
        Objects.requireNonNull(url);
        String schema = FileUtils.readFileUtf8(new File(url.getFile()));

        for (Map.Entry<String, String> replaceVar : replaceVars.entrySet()) {
            schema = schema.replace(replaceVar.getKey(), replaceVar.getValue());
        }

        return Environment.parse(schema);
    }
}
