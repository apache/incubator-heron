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

package org.apache.storm.topology.base;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IWindowedBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TupleFieldTimestampExtractor;
import org.apache.storm.tuple.TupleImpl;
import org.apache.storm.windowing.TimestampExtractor;

public abstract class BaseWindowedBolt implements IWindowedBolt {
  private static final long serialVersionUID = -3998164228343123590L;
  protected final transient org.apache.heron.api.windowing.WindowingConfigs windowConfiguration;
  protected org.apache.heron.api.windowing.TimestampExtractor timestampExtractor;

  /**
   * Holds a count value for count based windows and sliding intervals.
   */
  public static class Count implements Serializable {
    private static final long serialVersionUID = -2290882388716246812L;
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

  /**
   * Holds a Time duration for time based windows and sliding intervals.
   */
  public static class Duration implements Serializable {
    private static final long serialVersionUID = 5654070568075477148L;
    public final int value;

    public Duration(int value, TimeUnit timeUnit) {
      this.value = (int) timeUnit.toMillis(value);
    }

    /**
     * Returns a {@link Duration} corresponding to the the given value in milli seconds.
     *
     * @param milliseconds the duration in milliseconds
     * @return the Duration
     */
    public static Duration of(int milliseconds) {
      return new Duration(milliseconds, TimeUnit.MILLISECONDS);
    }

    /**
     * Returns a {@link Duration} corresponding to the the given value in days.
     *
     * @param days the number of days
     * @return the Duration
     */
    public static Duration days(int days) {
      return new Duration(days, TimeUnit.DAYS);
    }

    /**
     * Returns a {@link Duration} corresponding to the the given value in hours.
     *
     * @param hours the number of hours
     * @return the Duration
     */
    public static Duration hours(int hours) {
      return new Duration(hours, TimeUnit.HOURS);
    }

    /**
     * Returns a {@link Duration} corresponding to the the given value in minutes.
     *
     * @param minutes the number of minutes
     * @return the Duration
     */
    public static Duration minutes(int minutes) {
      return new Duration(minutes, TimeUnit.MINUTES);
    }

    /**
     * Returns a {@link Duration} corresponding to the the given value in seconds.
     *
     * @param seconds the number of seconds
     * @return the Duration
     */
    public static Duration seconds(int seconds) {
      return new Duration(seconds, TimeUnit.SECONDS);
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Duration duration = (Duration) o;

      return value == duration.value;

    }

    @Override
    public int hashCode() {
      return value;
    }

    @Override
    public String toString() {
      return "Duration{" + "value=" + value + '}';
    }
  }

  protected BaseWindowedBolt() {
    windowConfiguration = new org.apache.heron.api.windowing.WindowingConfigs();
  }

  private BaseWindowedBolt withWindowLength(Count count) {
    if (count.value <= 0) {
      throw new IllegalArgumentException("Window length must be positive [" + count + "]");
    }
    windowConfiguration.setTopologyBoltsWindowLengthCount(count.value);
    return this;
  }

  private BaseWindowedBolt withWindowLength(Duration duration) {
    if (duration.value <= 0) {
      throw new IllegalArgumentException("Window length must be positive [" + duration + "]");
    }
    windowConfiguration.setTopologyBoltsWindowLengthDurationMs(duration.value);
    return this;
  }

  private BaseWindowedBolt withSlidingInterval(Count count) {
    if (count.value <= 0) {
      throw new IllegalArgumentException("Sliding interval must be positive [" + count + "]");
    }
    windowConfiguration.setTopologyBoltsSlidingIntervalCount(count.value);
    return this;
  }

  private BaseWindowedBolt withSlidingInterval(Duration duration) {
    if (duration.value <= 0) {
      throw new IllegalArgumentException("Sliding interval must be positive [" + duration + "]");
    }
    windowConfiguration.setTopologyBoltsSlidingIntervalDurationMs(duration.value);
    return this;
  }

  /**
   * Tuple count based sliding window configuration.
   *
   * @param windowLength    the number of tuples in the window
   * @param slidingInterval the number of tuples after which the window slides
   */
  public BaseWindowedBolt withWindow(Count windowLength, Count slidingInterval) {
    return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
  }

  /**
   * Tuple count and time duration based sliding window configuration.
   *
   * @param windowLength    the number of tuples in the window
   * @param slidingInterval the time duration after which the window slides
   */
  public BaseWindowedBolt withWindow(Count windowLength, Duration slidingInterval) {
    return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
  }

  /**
   * Time duration and count based sliding window configuration.
   *
   * @param windowLength    the time duration of the window
   * @param slidingInterval the number of tuples after which the window slides
   */
  public BaseWindowedBolt withWindow(Duration windowLength, Count slidingInterval) {
    return withWindowLength(windowLength).withSlidingInterval(slidingInterval);
  }

  /**
   * Time duration based sliding window configuration.
   *
   * @param windowLength    the time duration of the window
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
   * Specify a field in the tuple that represents the timestamp as a long value. If this
   * field is not present in the incoming tuple, an {@link IllegalArgumentException} will be thrown.
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
    if (this.timestampExtractor != null) {
      throw new IllegalArgumentException(
          "Window is already configured with a timestamp extractor: " + timestampExtractor);
    }

    this.timestampExtractor = new org.apache.heron.api.windowing.TimestampExtractor() {

      @Override
      public long extractTimestamp(Tuple tuple) {
        return timestampExtractor.extractTimestamp(new TupleImpl(tuple));
      }
    };
    return this;
  }

  @Override
  public TimestampExtractor getTimestampExtractor() {
    return (this.timestampExtractor == null) ? null : new TimestampExtractor() {

      @Override
      public long extractTimestamp(org.apache.storm.tuple.Tuple tuple) {

        return timestampExtractor.extractTimestamp(new org.apache.heron.api.tuple.Tuple() {

          @Override
          public int size() {
            return tuple.size();
          }

          @Override
          public int fieldIndex(String field) {
            return tuple.fieldIndex(field);
          }

          @Override
          public boolean contains(String field) {
            return tuple.contains(field);
          }

          @Override
          public Object getValue(int i) {
            return tuple.getValue(i);
          }

          @Override
          public String getString(int i) {
            return tuple.getString(i);
          }

          @Override
          public Integer getInteger(int i) {
            return tuple.getInteger(i);
          }

          @Override
          public Long getLong(int i) {
            return tuple.getLong(i);
          }

          @Override
          public Boolean getBoolean(int i) {
            return tuple.getBoolean(i);
          }

          @Override
          public Short getShort(int i) {
            return tuple.getShort(i);
          }

          @Override
          public Byte getByte(int i) {
            return tuple.getByte(i);
          }

          @Override
          public Double getDouble(int i) {
            return tuple.getDouble(i);
          }

          @Override
          public Float getFloat(int i) {
            return tuple.getFloat(i);
          }

          @Override
          public byte[] getBinary(int i) {
            return tuple.getBinary(i);
          }

          @Override
          public Object getValueByField(String field) {
            return tuple.getValueByField(field);
          }

          @Override
          public String getStringByField(String field) {
            return tuple.getStringByField(field);
          }

          @Override
          public Integer getIntegerByField(String field) {
            return tuple.getIntegerByField(field);
          }

          @Override
          public Long getLongByField(String field) {
            return tuple.getLongByField(field);
          }

          @Override
          public Boolean getBooleanByField(String field) {
            return tuple.getBooleanByField(field);
          }

          @Override
          public Short getShortByField(String field) {
            return tuple.getShortByField(field);
          }

          @Override
          public Byte getByteByField(String field) {
            return tuple.getByteByField(field);
          }

          @Override
          public Double getDoubleByField(String field) {
            return tuple.getDoubleByField(field);
          }

          @Override
          public Float getFloatByField(String field) {
            return tuple.getFloatByField(field);
          }

          @Override
          public byte[] getBinaryByField(String field) {
            return tuple.getBinaryByField(field);
          }

          @Override
          public List<Object> getValues() {
            return tuple.getValues();
          }

          @Override
          public Fields getFields() {
            return new Fields(tuple.getFields().toList());
          }

          @Override
          public List<Object> select(Fields selector) {
            return tuple.select(new org.apache.storm.tuple.Fields(selector.toString()));
          }

          @Override
          public TopologyAPI.StreamId getSourceGlobalStreamId() {
            return TopologyAPI.StreamId.newBuilder().setId(tuple.getSourceStreamId())
                .setComponentName(tuple.getSourceComponent()).build();
          }

          @Override
          public String getSourceComponent() {
            return tuple.getSourceComponent();
          }

          @Override
          public int getSourceTask() {
            return tuple.getSourceTask();
          }

          @Override
          public String getSourceStreamId() {
            return tuple.getSourceStreamId();
          }

          @Override
          public void resetValues() {
            tuple.resetValues();
          }
        });
      }
    };
  }

  /**
   * Specify a stream id on which late tuples are going to be emitted.
   * It must be defined on a per-component basis, and in conjunction with the
   * {@link BaseWindowedBolt#withTimestampField}, otherwise {@link IllegalArgumentException} will
   * be thrown.
   *
   * @param streamId the name of the stream used to emit late tuples on
   */
  public BaseWindowedBolt withLateTupleStream(String streamId) {
    windowConfiguration.setTopologyBoltsLateTupleStream(streamId);
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
    windowConfiguration.setTopologyBoltsTupleTimestampMaxLagMs(duration.value);
    return this;
  }

  /**
   * Specify the watermark event generation interval. For tuple based timestamps, watermark events
   * are used to track the progress of time
   *
   * @param interval the interval at which watermark events are generated
   */
  public BaseWindowedBolt withWatermarkInterval(Duration interval) {
    windowConfiguration.setTopologyBoltsWatermarkEventIntervalMs(interval.value);
    return this;
  }

  @Override
  public void prepare(Map topoConf, TopologyContext context, OutputCollector
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
