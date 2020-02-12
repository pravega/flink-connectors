/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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