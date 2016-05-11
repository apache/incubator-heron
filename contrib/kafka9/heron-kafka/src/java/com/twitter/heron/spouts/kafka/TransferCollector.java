/*
 * Copyright 2016 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.heron.spouts.kafka;

import com.twitter.heron.api.metric.CountMetric;
import com.twitter.heron.api.metric.IMetric;
import com.twitter.heron.api.spout.ISpoutOutputCollector;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Threadsafe collector which can be used to transfer message from async message-fetcher to
 * spout main-thread which calls nextTuple. Note that, collector doesn't guarantee that
 * any emitted message will be read.
 * Note: Counters are not thread-safe. No attempts are made to synchronize counter values.
 */
public class TransferCollector extends LinkedBlockingQueue<TransferCollector.EmitData>
        implements ISpoutOutputCollector, IMetric, Serializable {
    public static final Logger LOG = Logger.getLogger(TransferCollector.class.getName());

    public static class EmitData implements Serializable {
        public String streamId;
        public Object messageId;
        public List<Object> tuple;

        public EmitData(String streamId, List<Object> tuple, Object messageId) {
            this.streamId = streamId;
            this.messageId = messageId;
            this.tuple = tuple;
        }

        /**
         * No argument contructor.
         */
        public EmitData() {
        }
    }

    private final CountMetric getEmitItemsTimeoutCounter;

    private final CountMetric getEmitItemsCallCounter;

    private final CountMetric getEmitCallCounter;

    private final CountMetric exceptionCounter;

    /**
     * Collector to serialize multiple emit and make it thread safe.
     *
     * @param emitQueueMaxSize Max size of emit buffer.
     */
    public TransferCollector(int emitQueueMaxSize) {
        super(emitQueueMaxSize);
        getEmitItemsTimeoutCounter = new CountMetric();
        getEmitItemsCallCounter = new CountMetric();
        getEmitCallCounter = new CountMetric();
        exceptionCounter = new CountMetric();
    }

    /**
     * Emits to in-memory thread-safe pending queue.
     */
    @Override
    public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
        getEmitCallCounter.incr();
        // Block until emit is successful in adding item to the transfer collector.
        try {
            put(new EmitData(streamId, tuple, messageId));
        } catch (InterruptedException ie) {
            exceptionCounter.incr();
        }
        return new ArrayList<Integer>();  // No task receive message directly from this collector.
    }

    @Override
    public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
        throw new IllegalStateException("Transfer collectors are not supposed to emit directly");
    }

    @Override
    public void reportError(Throwable throwable) {
        throw new IllegalStateException("Transfer collectors are not to transfer error messages");
    }

    @Override
    public Object getValueAndReset() {
        HashMap<String, Object> metrics = new HashMap<String, Object>();
        metrics.put("pendingEmits", size());
        metrics.put("getEmitItemsTimeoutCounter", getEmitItemsTimeoutCounter.getValueAndReset());
        metrics.put("getEmitItemsCallCounter", getEmitItemsCallCounter.getValueAndReset());
        metrics.put("getEmitCallCounter", getEmitCallCounter.getValueAndReset());
        metrics.put("exceptionCounter", exceptionCounter.getValueAndReset());
        return metrics;
    }

    // Accessor methods.

    /**
     * Get an item to emit from emit queue with timeout.
     *
     * @param timeout Duration of timeout.
     * @param unit    Unit of timeout.
     * @return First item from emit queue. Null if times out.
     */
    public EmitData getEmitItems(long timeout, TimeUnit unit) {
        getEmitItemsCallCounter.incr();
        try {
            EmitData item = poll(timeout, unit);
            if (item == null) {
                getEmitItemsTimeoutCounter.incr();
            }
            return item;
        } catch (InterruptedException ie) {
            exceptionCounter.incr();
            return null;
        }
    }

    /**
     * Poll with immediate return. Removes returned item from queue.
     *
     * @return First item on emit queue. Null if empty.
     */
    public EmitData getEmitItems() {
        getEmitItemsCallCounter.incr();
        return poll();
    }
}
