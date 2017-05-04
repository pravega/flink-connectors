/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 */
package io.pravega.connectors.flink.utils;

import org.apache.flink.runtime.execution.SuppressRestartsException;

/**
 * Special marker exception that aborts a program but indicates success.
 * Workaround for finite tests on infinite inputs.
 * 
 * <p>The SuccessException suppresses restarts (because, well, we don't want to recover
 * from success).
 */
public class SuccessException extends SuppressRestartsException {

    public SuccessException() {
        super(null);
    }
}