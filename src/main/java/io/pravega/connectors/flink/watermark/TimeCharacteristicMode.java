/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.connectors.flink.watermark;

import java.io.Serializable;

public enum TimeCharacteristicMode implements Serializable {

    /**
     * Processing time means that the source function will not carry any timestamp and watermark
     * information, and uses the system clock of the machine to determine the current time of the
     * data stream.
     */
    PROCESSING_TIME,


    /**
     * Event time means that the source function will need a timestamp of each individual element
     * in the stream (also called event) is determined by the event's individual custom timestamp.
     * (@link TimestampExtractor).
     *
     * <p>Operators that window or order data with respect to event time must buffer data until they
     * can be sure that all timestamps for a certain time interval have been received. This is
     * handled by the "watermarks" periodically emitted from the source function.
     *
     */
    EVENT_TIME,
}
