/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.connectors.flink.utils;

public class IntentionalException extends Exception {
    public IntentionalException(String message) {
        super(message);
    }
}
