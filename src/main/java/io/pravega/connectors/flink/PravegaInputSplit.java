/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink;

import com.google.common.base.Preconditions;
import io.pravega.client.segment.impl.Segment;
import org.apache.flink.core.io.InputSplit;

/**
 * A {@link PravegaInputSplit} corresponds to a Pravega {@link Segment}.
 */
public class PravegaInputSplit implements InputSplit {

    private final int splitId;

    private final Segment segment;

    private final long startOffset;

    // the value is not inclusive when batch client is reading the data for the segment
    private final long endOffset;

    public PravegaInputSplit(int splitId, Segment segment, long startOffset, long endOffset) {
        Preconditions.checkArgument(splitId >= 0, "The splitId is not recognizable.");
        Preconditions.checkNotNull(segment, "segment");
        Preconditions.checkArgument(
                startOffset >= 0,
                "The start offset is not recognizable.");
        Preconditions.checkArgument(
                startOffset <= endOffset, // keeping equals check assuming both start and end could be 0?
                "The end offset must be larger or equal to the start offset.");

        this.splitId = splitId;
        this.segment = segment;
        this.startOffset = startOffset;
        this.endOffset = endOffset;
    }

    @Override
    public int getSplitNumber() {
        return splitId;
    }

    public Segment getSegment() {
        return segment;
    }

    public long getStartOffset() {
        return startOffset;
    }

    public long getEndOffset() {
        return endOffset;
    }

    // --------------------------------------------------------------------

    @Override
    public int hashCode() {
        int result = splitId;
        result = 31 * result + segment.hashCode();
        result = 31 * result + Long.hashCode(startOffset);
        result = 31 * result + Long.hashCode(endOffset);

        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        } else if (obj instanceof PravegaInputSplit) {
            PravegaInputSplit other = (PravegaInputSplit) obj;

            return this.splitId == other.splitId &&
                    this.segment.equals(other.segment) &&
                    this.startOffset == other.startOffset &&
                    this.endOffset == other.endOffset;
        } else {
            return false;
        }
    }

    @Override
    public String toString() {
        return "PravegaInputSplit {" +
                "splitId = " + splitId +
                ", segment = " + segment.toString() +
                ", startOffset = " + startOffset +
                ", endOffset = " + endOffset +  "}";
    }
}
