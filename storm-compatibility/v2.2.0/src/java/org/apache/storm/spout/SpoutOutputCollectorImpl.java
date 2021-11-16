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

package org.apache.storm.spout;

import java.util.List;

import org.apache.storm.utils.Utils;

/**
 * This output collector exposes the API for emitting tuples from an {@link org.apache.storm.topology.IRichSpout}.
 * The main difference between this output collector and
 * {@link org.apache.storm.task.OutputCollector}
 * for {@link org.apache.storm.topology.IRichBolt} is that spouts can tag messages with ids so that they can be
 * acked or failed later on. This is the Spout portion of Storm's API to
 * guarantee that each message is fully processed at least once.
 */
public class SpoutOutputCollectorImpl extends SpoutOutputCollector {
  private org.apache.heron.api.spout.SpoutOutputCollector delegate;

  public SpoutOutputCollectorImpl(org.apache.heron.api.spout.SpoutOutputCollector delegate) {
    super(null);
    this.delegate = delegate;
  }

  @Override
  public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
    return delegate.emit(streamId, tuple, messageId);
  }

  @Override
  public List<Integer> emit(List<Object> tuple, Object messageId) {
    return emit(Utils.DEFAULT_STREAM_ID, tuple, messageId);
  }

  @Override
  public List<Integer> emit(List<Object> tuple) {
    return emit(tuple, null);
  }

  @Override
  public List<Integer> emit(String streamId, List<Object> tuple) {
    return emit(streamId, tuple, null);
  }

  @Override
  public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
    delegate.emitDirect(taskId, streamId, tuple, messageId);
  }

  @Override
  public void emitDirect(int taskId, List<Object> tuple, Object messageId) {
    emitDirect(taskId, Utils.DEFAULT_STREAM_ID, tuple, messageId);
  }

  @Override
  public void emitDirect(int taskId, String streamId, List<Object> tuple) {
    emitDirect(taskId, streamId, tuple, null);
  }

  @Override
  public void emitDirect(int taskId, List<Object> tuple) {
    emitDirect(taskId, tuple, null);
  }

  @Override
  public void reportError(Throwable error) {
    delegate.reportError(error);
  }
}
