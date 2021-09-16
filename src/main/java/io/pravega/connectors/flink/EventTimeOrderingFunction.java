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
package io.pravega.connectors.flink;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * Orders elements into event time order using the managed timestamp state.
 * Buffer the elements if they come early.
 * The type of the key is String.
 *
 * @param <T> The input type of the function.
 */
public class EventTimeOrderingFunction<T> extends KeyedProcessFunction<String, T, T> {

    private static final long serialVersionUID = 1L;

    private static final String EVENT_QUEUE_STATE_NAME = "eventQueue";

    private static final String LAST_TRIGGERING_TS_STATE_NAME = "lastTriggeringTsState";

    /**
     * The input type information for buffering events to managed state.
     */
    private final TypeInformation<T> typeInformation;

    /**
     * State to buffer all the data between watermarks.
     */
    private transient MapState<Long, List<T>> dataState;

    /**
     * State to keep the last triggering timestamp. Used to filter late events.
     */
    private transient ValueState<Long> lastTriggeringTsState;

    public EventTimeOrderingFunction(TypeInformation<T> typeInformation) {
        this.typeInformation = typeInformation;
    }

    @Override
    public void open(Configuration config) throws Exception {
        super.open(config);

        // create a map-based queue to buffer input elements
        MapStateDescriptor<Long, List<T>> elementQueueStateDescriptor = new MapStateDescriptor<>(
                EVENT_QUEUE_STATE_NAME,
                BasicTypeInfo.LONG_TYPE_INFO,
                new ListTypeInfo<>(this.typeInformation)
        );
        dataState = getRuntimeContext().getMapState(elementQueueStateDescriptor);

        // maintain a timestamp so anything before this time will be ignored
        ValueStateDescriptor<Long> lastTriggeringTsDescriptor =
                new ValueStateDescriptor<>(LAST_TRIGGERING_TS_STATE_NAME, Long.class);
        lastTriggeringTsState = getRuntimeContext().getState(lastTriggeringTsDescriptor);
    }

    @Override
    public void processElement(T element, Context ctx, Collector<T> out) throws Exception {
        // timestamp of the processed element
        Long timestamp = ctx.timestamp();
        if (timestamp == null) {
            // Simply forward the elements when the time characteristic of the program is set to ProcessingTime.
            out.collect(element);
            return;
        }

        // In event-time processing we assume correctness of the watermark.
        // Events with timestamp smaller than (or equal to) the last seen watermark are considered late.
        // FUTURE: emit late elements to a side output

        Long lastTriggeringTs = lastTriggeringTsState.value();

        // check if the element is late and drop it if it is late
        if (lastTriggeringTs == null || timestamp > lastTriggeringTs) {
            List<T> elementsForTimestamp = dataState.get(timestamp);

            if (elementsForTimestamp == null) {
                elementsForTimestamp = new ArrayList<>(1);

                // register event time timer, so the list will be outputted then
                ctx.timerService().registerEventTimeTimer(timestamp);
            }

            elementsForTimestamp.add(element);

            dataState.put(timestamp, elementsForTimestamp);
        }
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<T> out) throws Exception {
        // gets all elements for the triggering timestamps
        List<T> elements = dataState.get(timestamp);

        if (elements != null) {
            // emit elements in order
            elements.forEach(out::collect);

            // remove emitted elements from state
            dataState.remove(timestamp);

            // update the latest processing time
            lastTriggeringTsState.update(timestamp);
        }
    }
}
