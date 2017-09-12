/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.topology;

import java.util.Map;
import java.util.logging.Logger;

import org.apache.storm.generated.GlobalStreamId;
import org.apache.storm.generated.Grouping;
import org.apache.storm.grouping.CustomStreamGrouping;
import org.apache.storm.grouping.CustomStreamGroupingDelegate;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

public class BoltDeclarerImpl implements BoltDeclarer {
  private static final Logger LOG = Logger.getLogger(BoltDeclarerImpl.class.getName());

  private com.twitter.heron.api.topology.BoltDeclarer delegate;

  public BoltDeclarerImpl(com.twitter.heron.api.topology.BoltDeclarer delegate) {
    this.delegate = delegate;
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public BoltDeclarer addConfigurations(Map conf) {
    delegate.addConfigurations((Map<String, Object>) conf);
    return this;
  }

  @Override
  public BoltDeclarer addConfiguration(String config, Object value) {
    delegate.addConfiguration(config, value);
    return this;
  }

  @Override
  public BoltDeclarer setDebug(boolean debug) {
    delegate.setDebug(debug);
    return this;
  }

  @Override
  public BoltDeclarer setMaxTaskParallelism(Number val) {
    // Heron does not support this
    return this;
  }

  @Override
  public BoltDeclarer setMaxSpoutPending(Number val) {
    delegate.setMaxSpoutPending(val);
    return this;
  }

  @Override
  public BoltDeclarer setNumTasks(Number val) {
    // Heron does not support this
    return this;
  }

  @Override
  public BoltDeclarer fieldsGrouping(String componentId, Fields fields) {
    return fieldsGrouping(componentId, Utils.DEFAULT_STREAM_ID, fields);
  }

  @Override
  public BoltDeclarer fieldsGrouping(String componentId, String streamId, Fields fields) {
    delegate.fieldsGrouping(componentId, streamId, fields.getDelegate());
    return this;
  }

  @Override
  public BoltDeclarer globalGrouping(String componentId) {
    return globalGrouping(componentId, Utils.DEFAULT_STREAM_ID);
  }

  @Override
  public BoltDeclarer globalGrouping(String componentId, String streamId) {
    delegate.globalGrouping(componentId, streamId);
    return this;
  }

  @Override
  public BoltDeclarer shuffleGrouping(String componentId) {
    return shuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
  }

  @Override
  public BoltDeclarer shuffleGrouping(String componentId, String streamId) {
    delegate.shuffleGrouping(componentId, streamId);
    return this;
  }

  @Override
  public BoltDeclarer localOrShuffleGrouping(String componentId) {
    return localOrShuffleGrouping(componentId, Utils.DEFAULT_STREAM_ID);
  }

  @Override
  public BoltDeclarer localOrShuffleGrouping(String componentId, String streamId) {
    delegate.localOrShuffleGrouping(componentId, streamId);
    return this;
  }

  @Override
  public BoltDeclarer noneGrouping(String componentId) {
    return noneGrouping(componentId, Utils.DEFAULT_STREAM_ID);
  }

  @Override
  public BoltDeclarer noneGrouping(String componentId, String streamId) {
    delegate.noneGrouping(componentId, streamId);
    return this;
  }

  @Override
  public BoltDeclarer allGrouping(String componentId) {
    return allGrouping(componentId, Utils.DEFAULT_STREAM_ID);
  }

  @Override
  public BoltDeclarer allGrouping(String componentId, String streamId) {
    delegate.allGrouping(componentId, streamId);
    return this;
  }

  @Override
  public BoltDeclarer directGrouping(String componentId) {
    return directGrouping(componentId, Utils.DEFAULT_STREAM_ID);
  }

  @Override
  public BoltDeclarer directGrouping(String componentId, String streamId) {
    delegate.directGrouping(componentId, streamId);
    return this;
  }

  @Override
  public BoltDeclarer customGrouping(String componentId, CustomStreamGrouping grouping) {
    return customGrouping(componentId, Utils.DEFAULT_STREAM_ID, grouping);
  }

  @Override
  public BoltDeclarer customGrouping(
      String componentId, String streamId, CustomStreamGrouping grouping) {
    delegate.customGrouping(componentId, streamId, new CustomStreamGroupingDelegate(grouping));
    return this;
  }

  @Override
  public BoltDeclarer partialKeyGrouping(String componentId, Fields fields) {
    throw new RuntimeException("partialKeyGrouping not supported");
  }

  @Override
  public BoltDeclarer partialKeyGrouping(String componentId, String streamId, Fields fields) {
    throw new RuntimeException("partialKeyGrouping not supported");
  }

  @Override
  public BoltDeclarer grouping(GlobalStreamId id, Grouping grouping) {
    switch (grouping.getSetField()) {
      case ALL:
        return allGrouping(id.get_componentId(), id.get_streamId());
      case DIRECT:
        return directGrouping(id.get_componentId(), id.get_streamId());
      case FIELDS:
        return fieldsGrouping(id.get_componentId(), id.get_streamId(), new Fields(grouping.get_fields()));
      case LOCAL_OR_SHUFFLE:
        return localOrShuffleGrouping(id.get_componentId(), id.get_streamId());
      case SHUFFLE:
        return shuffleGrouping(id.get_componentId(), id.get_streamId());
      case NONE:
        return noneGrouping(id.get_componentId(), id.get_streamId());
      case CUSTOM_SERIALIZED:
        grouping.get_custom_serialized();
        LOG.warning(String.format(
            "%s.grouping(GlobalStreamId id, Grouping grouping) not supported for %s, swapping in "
                + "noneGrouping. The tuple stream routing will be broken for streamId %s",
            getClass().getName(), grouping.getSetField(), id));
        return noneGrouping(id.get_componentId(), id.get_streamId());
      case CUSTOM_OBJECT:
        //grouping.get_custom_object();
      default:
        throw new RuntimeException(
            "grouping(GlobalStreamId id, Grouping grouping) not supported for "
                + grouping.getSetField());
    }
  }

  @Override
  public BoltDeclarer setMemoryLoad(Number onHeap) {
    return null;
  }

  @Override
  public BoltDeclarer setMemoryLoad(Number onHeap, Number offHeap) {
    return null;
  }

  @Override
  public BoltDeclarer setCPULoad(Number amount) {
    return null;
  }
}
