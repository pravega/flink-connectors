/**
 * Copyright (c) Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.connectors.flink;

import com.google.common.base.Preconditions;
import io.pravega.client.batch.SegmentRange;
import org.apache.flink.core.io.InputSplit;

/**
 * A {@link PravegaInputSplit} corresponds to a Pravega {@link SegmentRange}.
 */
public class PravegaInputSplit implements InputSplit {

    private final int splitId;

    private final SegmentRange segmentRange;

    public PravegaInputSplit(int splitId, SegmentRange segmentRange) {
        Preconditions.checkArgument(splitId >= 0, "The splitId is not recognizable.");
        Preconditions.checkNotNull(segmentRange, "segmentRange");
        this.splitId = splitId;
        this.segmentRange = segmentRange;
    }

    @Override
    public int getSplitNumber() {
        return splitId;
    }

    public SegmentRange getSegmentRange() {
        return segmentRange;
    }

    // --------------------------------------------------------------------
    // constructor guards segment range from being null
    @Override
    public boolean equals(Object o) {

        if (this == o) {
            return true;
        }

        if (!(o instanceof PravegaInputSplit)) {
            return false;
        }

        PravegaInputSplit that = (PravegaInputSplit) o;

        if (!(this.getSegmentRange().equals(that.getSegmentRange()))) {
                return false;
        }

        return splitId == that.splitId;

    }

    @Override
    public int hashCode() {

        int prime = 59;
        int result = 1;

        result = result * prime + splitId;
        result = result * prime + Long.hashCode(getSegmentRange().getSegmentId());
        result = result * prime + Long.hashCode(getSegmentRange().getStartOffset());
        result = result * prime + Long.hashCode(getSegmentRange().getEndOffset());

        String scope = getSegmentRange().getScope();
        String stream = getSegmentRange().getStreamName();

        result = result * prime + (scope == null ? 43 : scope.hashCode());
        result = result * prime + (stream == null ? 43 : stream.hashCode());

        return result;
    }

    @Override
    public String toString() {
        return "PravegaInputSplit {" +
                "splitId = " + splitId +
                ", segmentRange = " + segmentRange.toString() + "}";
    }
}
