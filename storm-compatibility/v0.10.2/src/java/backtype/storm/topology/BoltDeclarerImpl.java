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

package backtype.storm.topology;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.grouping.CustomStreamGrouping;
import backtype.storm.grouping.CustomStreamGroupingDelegate;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.ConfigUtils;
import backtype.storm.utils.Utils;

public class BoltDeclarerImpl implements BoltDeclarer {
  private org.apache.heron.api.topology.BoltDeclarer delegate;

  public BoltDeclarerImpl(org.apache.heron.api.topology.BoltDeclarer delegate) {
    this.delegate = delegate;
  }

  @Override
  @SuppressWarnings({"rawtypes", "unchecked"})
  public BoltDeclarer addConfigurations(Map conf) {
    // Translate config to heron config and then apply.
    Map<String, Object> heronConf = ConfigUtils.translateComponentConfig(conf);
    delegate.addConfigurations(heronConf);
    return this;
  }

  @Override
  public BoltDeclarer addConfiguration(String config, Object value) {
    Map<String, Object> configMap = new HashMap<String, Object>();
    configMap.put(config, value);

    addConfigurations(configMap);
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
}
