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

import java.util.Map;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.spout.IRichSpout;

public class SpoutDeclarer extends BaseComponentDeclarer<SpoutDeclarer> {
  private OutputFieldsGetter output;

  public SpoutDeclarer(String name, IRichSpout spout, Number taskParallelism) {
    super(name, spout, taskParallelism);
    output = new OutputFieldsGetter();
    spout.declareOutputFields(output);
  }

  @Override
  public SpoutDeclarer returnThis() {
    return this;
  }

  public void dump(TopologyAPI.Topology.Builder bldr) {
    TopologyAPI.Spout.Builder spoutBldr = TopologyAPI.Spout.newBuilder();

    TopologyAPI.Component.Builder compBldr = TopologyAPI.Component.newBuilder();
    super.dump(compBldr);
    spoutBldr.setComp(compBldr);

    Map<String, TopologyAPI.StreamSchema.Builder> outs = output.getFieldsDeclaration();
    for (Map.Entry<String, TopologyAPI.StreamSchema.Builder> entry : outs.entrySet()) {
      TopologyAPI.OutputStream.Builder obldr = TopologyAPI.OutputStream.newBuilder();
      TopologyAPI.StreamId.Builder sbldr = TopologyAPI.StreamId.newBuilder();
      sbldr.setId(entry.getKey());
      sbldr.setComponentName(getName());
      obldr.setStream(sbldr);
      obldr.setSchema(entry.getValue());
      spoutBldr.addOutputs(obldr);
    }

    bldr.addSpouts(spoutBldr);
  }
}
