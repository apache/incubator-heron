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

import org.apache.heron.api.windowing.TimestampExtractor;

public abstract class BaseStatefulWindowedBolt<K extends Serializable, V extends Serializable>
    extends BaseWindowedBolt
    implements IStatefulWindowedBolt<K, V> {

  private static final long serialVersionUID = -5082068737902535908L;

  /**
   * {@inheritDoc}
   */
  @Override
  public BaseStatefulWindowedBolt<K, V> withWindow(BaseWindowedBolt.Count windowLength,
                                                   BaseWindowedBolt.Count slidingInterval) {
    super.withWindow(windowLength, slidingInterval);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BaseStatefulWindowedBolt<K, V> withWindow(BaseWindowedBolt.Count windowLength, Duration
      slidingInterval) {
    super.withWindow(windowLength, slidingInterval);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BaseStatefulWindowedBolt<K, V> withWindow(Duration windowLength,
                                                   BaseWindowedBolt.Count slidingInterval) {
    super.withWindow(windowLength, slidingInterval);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BaseStatefulWindowedBolt<K, V> withWindow(Duration windowLength, Duration
      slidingInterval) {
    super.withWindow(windowLength, slidingInterval);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BaseStatefulWindowedBolt<K, V> withWindow(BaseWindowedBolt.Count windowLength) {
    super.withWindow(windowLength);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BaseStatefulWindowedBolt<K, V> withWindow(Duration windowLength) {
    super.withWindow(windowLength);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BaseStatefulWindowedBolt<K, V> withTumblingWindow(BaseWindowedBolt.Count count) {
    super.withTumblingWindow(count);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BaseStatefulWindowedBolt<K, V> withTumblingWindow(Duration duration) {
    super.withTumblingWindow(duration);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BaseStatefulWindowedBolt<K, V> withTimestampField(String fieldName) {
    super.withTimestampField(fieldName);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BaseStatefulWindowedBolt<K, V> withTimestampExtractor(
      TimestampExtractor timestampExtractor) {
    super.withTimestampExtractor(timestampExtractor);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BaseStatefulWindowedBolt<K, V> withLateTupleStream(String streamName) {
    super.withLateTupleStream(streamName);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BaseStatefulWindowedBolt<K, V> withLag(Duration duration) {
    super.withLag(duration);
    return this;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public BaseStatefulWindowedBolt<K, V> withWatermarkInterval(Duration interval) {
    super.withWatermarkInterval(interval);
    return this;
  }

  @Override
  public void preSave(String checkpointId) {
    //NOOP
  }
}
