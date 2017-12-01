package io.pravega.connectors.flink.util;

import org.apache.commons.text.RandomStringGenerator;

public class RandomStringUtils {

    private static final RandomStringGenerator alphabeticGenerator = new RandomStringGenerator.Builder().withinRange('a', 'z').build();

    public static String randomAlphabetic(int length) {
        return alphabeticGenerator.generate(length);
    }
}
