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

// TODO:- Add this
// import org.apache.storm.grouping.CustomStreamGrouping;

import org.apache.storm.generated.StormTopology;
import org.apache.storm.windowing.TupleWindow;

import com.twitter.heron.api.HeronTopology;

public class TopologyBuilder {
  private com.twitter.heron.api.topology.TopologyBuilder delegate =
      new com.twitter.heron.api.topology.TopologyBuilder();

  public StormTopology createTopology() {
    HeronTopology topology = delegate.createTopology();
    return new StormTopology(topology);
  }

  public BoltDeclarer setBolt(String id, IRichBolt bolt) {
    return setBolt(id, bolt, null);
  }

  public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelismHint) {
    IRichBoltDelegate boltImpl = new IRichBoltDelegate(bolt);
    com.twitter.heron.api.topology.BoltDeclarer declarer =
        delegate.setBolt(id, boltImpl, parallelismHint);
    return new BoltDeclarerImpl(declarer);
  }

  public BoltDeclarer setBolt(String id, IBasicBolt bolt) {
    return setBolt(id, bolt, null);
  }

  public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelismHint) {
    return setBolt(id, new BasicBoltExecutor(bolt), parallelismHint);
  }

  public SpoutDeclarer setSpout(String id, IRichSpout spout) {
    return setSpout(id, spout, null);
  }

  public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelismHint) {
    IRichSpoutDelegate spoutImpl = new IRichSpoutDelegate(spout);
    com.twitter.heron.api.topology.SpoutDeclarer declarer =
        delegate.setSpout(id, spoutImpl, parallelismHint);
    return new SpoutDeclarerImpl(declarer);
  }

  /**
   * Define a new bolt in this topology. This defines a windowed bolt, intended
   * for windowing operations. The {@link IWindowedBolt#execute(TupleWindow)} method
   * is triggered for each window interval with the list of current events in the window.
   *
   * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
   * @param bolt the windowed bolt
   * @return use the returned object to declare the inputs to this component
   * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
   */
  public BoltDeclarer setBolt(String id, IWindowedBolt bolt) throws IllegalArgumentException {
    return setBolt(id, bolt, null);
  }

  /**
   * Define a new bolt in this topology. This defines a windowed bolt, intended
   * for windowing operations. The {@link IWindowedBolt#execute(TupleWindow)} method
   * is triggered for each window interval with the list of current events in the window.
   *
   * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
   * @param bolt the windowed bolt
   * @param parallelism_hint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somwehere around the cluster.
   * @return use the returned object to declare the inputs to this component
   * @throws IllegalArgumentException if {@code parallelism_hint} is not positive
   */
  public BoltDeclarer setBolt(String id, IWindowedBolt bolt, Number parallelism_hint) throws IllegalArgumentException {
    return setBolt(id, new WindowedBoltExecutor(bolt), parallelism_hint);
  }
}
