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

package org.apache.heron.api.topology;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.heron.api.bolt.IRichBolt;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.grouping.AllStreamGrouping;
import org.apache.heron.api.grouping.CustomStreamGrouping;
import org.apache.heron.api.grouping.DirectStreamGrouping;
import org.apache.heron.api.grouping.FieldsStreamGrouping;
import org.apache.heron.api.grouping.GlobalStreamGrouping;
import org.apache.heron.api.grouping.NoneStreamGrouping;
import org.apache.heron.api.grouping.ShuffleStreamGrouping;
import org.apache.heron.api.grouping.StreamGrouping;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.utils.Utils;

public class BoltDeclarer extends BaseComponentDeclarer<BoltDeclarer> {
  private OutputFieldsGetter output;
  private List<TopologyAPI.InputStream.Builder> inputs;

  public BoltDeclarer(String name, IRichBolt bolt, Number taskParallelism) {
    super(name, bolt, taskParallelism);
    inputs = new LinkedList<TopologyAPI.InputStream.Builder>();
    output = new OutputFieldsGetter();
    bolt.declareOutputFields(output);
  }

  @Override
  public BoltDeclarer returnThis() {
    return this;
  }

  public void dump(TopologyAPI.Topology.Builder bldr) {
    TopologyAPI.Bolt.Builder boltBldr = TopologyAPI.Bolt.newBuilder();

    TopologyAPI.Component.Builder cbldr = TopologyAPI.Component.newBuilder();
    super.dump(cbldr);
    boltBldr.setComp(cbldr);

    for (TopologyAPI.InputStream.Builder iter : inputs) {
      boltBldr.addInputs(iter);
    }

    Map<String, TopologyAPI.StreamSchema.Builder> outs = output.getFieldsDeclaration();
    for (Map.Entry<String, TopologyAPI.StreamSchema.Builder> entry : outs.entrySet()) {
      TopologyAPI.OutputStream.Builder obldr = TopologyAPI.OutputStream.newBuilder();
      TopologyAPI.StreamId.Builder sbldr = TopologyAPI.StreamId.newBuilder();
      sbldr.setId(entry.getKey());
      sbldr.setComponentName(getName());
      obldr.setStream(sbldr);
      obldr.setSchema(entry.getValue());
      boltBldr.addOutputs(obldr);
    }

    bldr.addBolts(boltBldr);
  }

  public BoltDeclarer fieldsGrouping(String componentName, Fields fields) {
    return fieldsGrouping(componentName, Utils.DEFAULT_STREAM_ID, fields);
  }

  public BoltDeclarer fieldsGrouping(String componentName, String streamId, Fields fields) {
    return grouping(componentName, streamId, new FieldsStreamGrouping(fields));
  }

  public BoltDeclarer globalGrouping(String componentName) {
    return globalGrouping(componentName, Utils.DEFAULT_STREAM_ID);
  }

  public BoltDeclarer globalGrouping(String componentName, String streamId) {
    return grouping(componentName, streamId, new GlobalStreamGrouping());
  }

  public BoltDeclarer shuffleGrouping(String componentName) {
    return shuffleGrouping(componentName, Utils.DEFAULT_STREAM_ID);
  }

  public BoltDeclarer shuffleGrouping(String componentName, String streamId) {
    return grouping(componentName, streamId, new ShuffleStreamGrouping());
  }

  public BoltDeclarer localOrShuffleGrouping(String componentName) {
    return localOrShuffleGrouping(componentName, Utils.DEFAULT_STREAM_ID);
  }

  public BoltDeclarer localOrShuffleGrouping(String componentName, String streamId) {
    // Heron tasks are process based, thus there's no concept of local(within process)
    // shuffling. So we map local grouping strategy to shuffleGrouping
    return shuffleGrouping(componentName, streamId);
  }

  public BoltDeclarer noneGrouping(String componentName) {
    return noneGrouping(componentName, Utils.DEFAULT_STREAM_ID);
  }

  public BoltDeclarer noneGrouping(String componentName, String streamId) {
    return grouping(componentName, streamId, new NoneStreamGrouping());
  }

  public BoltDeclarer allGrouping(String componentName) {
    return allGrouping(componentName, Utils.DEFAULT_STREAM_ID);
  }

  public BoltDeclarer allGrouping(String componentName, String streamId) {
    return grouping(componentName, streamId, new AllStreamGrouping());
  }

  public BoltDeclarer directGrouping(String componentName) {
    return directGrouping(componentName, Utils.DEFAULT_STREAM_ID);
  }

  public BoltDeclarer directGrouping(String componentName, String streamId) {
    return grouping(componentName, streamId, new DirectStreamGrouping());
  }

  public BoltDeclarer customGrouping(String componentName, CustomStreamGrouping grouper) {
    return customGrouping(componentName, Utils.DEFAULT_STREAM_ID, grouper);
  }

  public BoltDeclarer customGrouping(
      String componentName,
      String streamId,
      CustomStreamGrouping grouper) {

    return grouping(componentName, streamId, grouper);
  }

  public BoltDeclarer grouping(String componentName, StreamGrouping grouper) {
    return grouping(componentName, Utils.DEFAULT_STREAM_ID, grouper);
  }

  public BoltDeclarer grouping(
      String componentName,
      String streamId,
      StreamGrouping grouper) {

    inputs.add(grouper.buildStream(componentName, streamId));
    return this;
  }
}
