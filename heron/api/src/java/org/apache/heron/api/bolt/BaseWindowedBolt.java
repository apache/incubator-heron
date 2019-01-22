/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.api.bolt;

import java.io.Serializable;
import java.time.Duration;
import java.util.Map;

import org.apache.heron.api.topology.OutputFieldsDeclarer;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.api.windowing.EvictionPolicy;
import org.apache.heron.api.windowing.TimestampExtractor;
import org.apache.heron.api.windowing.TriggerPolicy;
import org.apache.heron.api.windowing.TupleFieldTimestampExtractor;
import org.apache.heron.api.windowing.WindowingConfigs;

public abstract class BaseWindowedBolt implements IWindowedBolt {
  private static final long serialVersionUID = 5688213068448231559L;

  protected final transient WindowingConfigs windowConfiguration;
  protected TimestampExtractor timestampExtractor;

  /**
   * Holds a count value for count based windows and sliding intervals.
   */
  public static class Count implements Serializable {
    private static final long serialVersionUID = -6420372171917099657L;
    public final int value;

    public Count(int value) {
      this.value = value;
    }

    /**
     * Returns a {@link Count} of given value.
     *
     * @param value the count value
     * @return the Count
     */
    public static Count of(int value) {
      return new Count(value);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Count count = (Count) o;

      return value == count.value;

    }

    @Override
    public int hashCode() {
      return value;
    }

    @Override
    public String toString() {
      return "Count{" + "value=" + value + '}';
    }
  }

  protected BaseWindowedBolt() {
    windowConfiguration = new WindowingConfigs();
  }

  private BaseWindowedBolt withWindowLength(Count count) {
    if (count == null) {
      throw new IllegalArgumentException("Window length cannot be set null");
    }
    if (count.value <= 0) {
      throw new IllegalArgumentException("Window length must be positive [" + count + "]");
    }
    windowConfiguration.put(WindowingConfigs.TOPOLOGY_BOLTS_WINDOW_LENGTH_COUNT, count.value);
    return this;
  }

  private BaseWindowedBolt withWindowLength(Duration duration) {
    if (duration == null) {
      throw new IllegalArgumentException("Window length cannot be set null");
    }
    if (duration.isNegative() || duration.isZero()) {
      throw new IllegalArgumentException("Window length must be positive [" + duration + "]");
    }

    windowConfiguration.put(WindowingConfigs.TOPOLOGY_BOLTS_WINDOW_LENGTH_DURATION_MS,
        duration.toMillis());
    return this;
  }

  private BaseWindowedBolt withSlidingInterval(Count count) {
    if (count == null) {
      throw new IllegalArgumentException("Sliding interval cannot be set null");
    }
    if (count.value <= 0) {
      throw new IllegalArgumentException("Sliding interval must be positive [" + count + "]");
    }
    windowConfiguration.put(WindowingConfigs.TOPOLOGY_BOLTS_SLIDING_INTERVAL_COUNT, count.value);
    return this;
  }

  private BaseWindowedBolt withSlidingInterval(Duration duration) {
    if (duration == null) {
      throw new IllegalArgumentException("Sliding interval cannot be set null");
    }
    if (duration.isNegative() || duration.isZero()) {
      throw new IllegalArgumentException("Sliding interval must be positive [" + duration + "]");
    }
    windowConfiguration.put(WindowingConfigs.TOPOLOGY_BOLTS_SLIDING_INTERVAL_DURATION_MS,
        duration.toMillis());
    return this;
  }

  /**
   * Tuple count based sliding window configuration.
   *
   * @param windowLength the number of tuples in the window
   * @param slidingInterval the number of tuples after which the window slides
   */
  public BaseWindowedBolt withWindow(Count windowLength, Count slidingInterval) {
    return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
  }

  /**
   * Tuple count and time duration based sliding window configuration.
   *
   * @param windowLength the number of tuples in the window
   * @param slidingInterval the time duration after which the window slides
   */
  public BaseWindowedBolt withWindow(Count windowLength, Duration slidingInterval) {
    return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
  }

  /**
   * Time duration and count based sliding window configuration.
   *
   * @param windowLength the time duration of the window
   * @param slidingInterval the number of tuples after which the window slides
   */
  public BaseWindowedBolt withWindow(Duration windowLength, Count slidingInterval) {
    return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
  }

  /**
   * Time duration based sliding window configuration.
   *
   * @param windowLength the time duration of the window
   * @param slidingInterval the time duration after which the window slides
   */
  public BaseWindowedBolt withWindow(Duration windowLength, Duration slidingInterval) {
    return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
  }

  /**
   * A tuple count based window that slides with every incoming tuple.
   *
   * @param windowLength the number of tuples in the window
   */
  public BaseWindowedBolt withWindow(Count windowLength) {
    return withWindowLength(windowLength).withSlidingInterval(new Count(1));
  }

  /**
   * A time duration based window that slides with every incoming tuple.
   *
   * @param windowLength the time duration of the window
   */
  public BaseWindowedBolt withWindow(Duration windowLength) {
    return withWindowLength(windowLength).withSlidingInterval(new Count(1));
  }

  /**
   * A count based tumbling window.
   *
   * @param count the number of tuples after which the window tumbles
   */
  public BaseWindowedBolt withTumblingWindow(Count count) {
    return withWindowLength(count).withSlidingInterval(count);
  }

  /**
   * A time duration based tumbling window.
   *
   * @param duration the time duration after which the window tumbles
   */
  public BaseWindowedBolt withTumblingWindow(Duration duration) {
    return withWindowLength(duration).withSlidingInterval(duration);
  }

  /**
   * Specify a field in the tuple that represents the timestamp as a long value. The timestamp
   * should also be in milliseconds. If this field is not present in the
   * incoming tuple, an {@link IllegalArgumentException} will be thrown.
   *
   * @param fieldName the name of the field that contains the timestamp
   */
  public BaseWindowedBolt withTimestampField(String fieldName) {
    return withTimestampExtractor(TupleFieldTimestampExtractor.of(fieldName));
  }

  /**
   * Specify the timestamp extractor implementation.
   *
   * @param timestampExtractor the {@link TimestampExtractor} implementation
   */
  @SuppressWarnings("HiddenField")
  public BaseWindowedBolt withTimestampExtractor(TimestampExtractor timestampExtractor) {
    if (timestampExtractor == null) {
      throw new IllegalArgumentException("Timestamp extractor cannot be set to null");
    }
    if (this.timestampExtractor != null) {
      throw new IllegalArgumentException(
          "Window is already configured with a timestamp " + "extractor: " + timestampExtractor);
    }
    this.timestampExtractor = timestampExtractor;
    return this;
  }

  @Override
  public TimestampExtractor getTimestampExtractor() {
    return timestampExtractor;
  }

  /**
   * Specify a stream id on which late tuples are going to be emitted. They are going to be
   * accessible via the
   * {@link WindowedBoltExecutor#LATE_TUPLE_FIELD} field.
   * It must be defined on a per-component basis, and in conjunction with the
   * {@link BaseWindowedBolt#withTimestampField}, otherwise {@link IllegalArgumentException} will
   * be thrown.
   *
   * @param streamId the name of the stream used to emit late tuples on
   */
  public BaseWindowedBolt withLateTupleStream(String streamId) {
    if (streamId == null) {
      throw new IllegalArgumentException("Cannot set late tuple stream id to null");
    }
    windowConfiguration.put(WindowingConfigs.TOPOLOGY_BOLTS_LATE_TUPLE_STREAM, streamId);
    return this;
  }


  /**
   * Specify the maximum time lag of the tuple timestamp in milliseconds. It means that the tuple
   * timestamps
   * cannot be out of order by more than this amount.
   *
   * @param duration the max lag duration
   */
  public BaseWindowedBolt withLag(Duration duration) {
    if (duration == null) {
      throw new IllegalArgumentException("Lag duration cannot be set null");
    }
    if (duration.isNegative() || duration.isZero()) {
      throw new IllegalArgumentException("Lag duration must be positive [" + duration + "]");
    }
    windowConfiguration.put(WindowingConfigs.TOPOLOGY_BOLTS_TUPLE_TIMESTAMP_MAX_LAG_MS,
        duration.toMillis());
    return this;
  }

  /**
   * Specify the watermark event generation interval. For tuple based timestamps, watermark events
   * are used to track the progress of time
   *
   * @param interval the interval at which watermark events are generated
   */
  public BaseWindowedBolt withWatermarkInterval(Duration interval) {
    if (interval == null) {
      throw new IllegalArgumentException("Watermark interval cannot be set null");
    }
    if (interval.isNegative() || interval.isZero()) {
      throw new IllegalArgumentException("Watermark interval must be positive [" + interval + "]");
    }
    windowConfiguration.put(WindowingConfigs.TOPOLOGY_BOLTS_WATERMARK_EVENT_INTERVAL_MS,
        interval.toMillis());
    return this;
  }

  /**
   * Sets a custom eviction policy to use for this bolt
   *
   * @param evictionPolicy the eviction policy to use
   * @return this
   */
  public BaseWindowedBolt withCustomEvictor(EvictionPolicy<Tuple, ?> evictionPolicy) {
    windowConfiguration.setTopologyBoltsWindowCustomEvictor(evictionPolicy);
    return this;
  }

  /**
   * Sets a custom trigger policy to use for this bolt
   *
   * @param triggerPolicy the trigger policy to use
   * @return this
   */
  public BaseWindowedBolt withCustomTrigger(TriggerPolicy<Tuple, ?> triggerPolicy) {
    windowConfiguration.setTopologyBoltsWindowCustomTrigger(triggerPolicy);
    return this;
  }

  @Override
  public void prepare(Map<String, Object> topoConf, TopologyContext context, OutputCollector
      collector) {
    // NOOP
  }

  @Override
  public void cleanup() {
    // NOOP
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    // NOOP
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    return windowConfiguration;
  }
}
