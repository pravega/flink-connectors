/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.pravega.watermark;

import io.pravega.client.stream.TimeWindow;
import org.apache.flink.streaming.api.watermark.Watermark;

public abstract class LowerBoundAssigner<T> implements AssignerWithTimeWindows<T> {

    private static final long serialVersionUID = 2069173720413829850L;

    public LowerBoundAssigner() {
    }

    @Override
    public abstract long extractTimestamp(T element, long previousElementTimestamp);

    // built-in watermark implementation which emits the lower bound - 1
    @Override
    public Watermark getWatermark(TimeWindow timeWindow) {
        // There is no LowerBound watermark if we're near the head of the stream
        if (timeWindow == null || timeWindow.isNearHeadOfStream()) {
            return null;
        }
        return timeWindow.getLowerTimeBound() == Long.MIN_VALUE ?
                new Watermark(Long.MIN_VALUE) :
                new Watermark(timeWindow.getLowerTimeBound() - 1L);
    }
}

