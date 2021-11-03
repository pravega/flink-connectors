/**
 * Copyright Pravega Authors.
 *
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
package io.pravega.connectors.flink.watermark;

import io.pravega.client.stream.TimeWindow;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.streaming.api.watermark.Watermark;

import javax.annotation.Nullable;
import java.io.Serializable;

@Public
public interface AssignerWithTimeWindows<T> extends TimestampAssigner<T>, Serializable {

    // app must implement the timestamp extraction
    @Nullable
    Watermark getWatermark(TimeWindow window);
}
