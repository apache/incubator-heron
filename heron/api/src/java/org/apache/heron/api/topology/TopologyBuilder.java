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

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import org.apache.heron.api.HeronTopology;
import org.apache.heron.api.bolt.BasicBoltExecutor;
import org.apache.heron.api.bolt.IBasicBolt;
import org.apache.heron.api.bolt.IRichBolt;
import org.apache.heron.api.bolt.IStatefulWindowedBolt;
import org.apache.heron.api.bolt.IWindowedBolt;
import org.apache.heron.api.bolt.StatefulWindowedBoltExecutor;
import org.apache.heron.api.bolt.WindowedBoltExecutor;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.spout.IRichSpout;
import org.apache.heron.api.state.State;
import org.apache.heron.api.windowing.TupleWindow;

/**
 * TopologyBuilder exposes the Java API for specifying a topology for Heron
 * to execute. Topologies are Thrift structures in the end, but since the Thrift API
 * is so verbose, TopologyBuilder greatly eases the process of creating topologies.
 * The template for creating and submitting a topology looks something like:
 * <p>
 * <pre>
 * TopologyBuilder builder = new TopologyBuilder();
 *
 * builder.setSpout("1", new TestWordSpout(true), 5);
 * builder.setSpout("2", new TestWordSpout(true), 3);
 * builder.setBolt("3", new TestWordCounter(), 3)
 *          .fieldsGrouping("1", new Fields("word"))
 *          .fieldsGrouping("2", new Fields("word"));
 * builder.setBolt("4", new TestGlobalCount())
 *          .globalGrouping("1");
 *
 * Map conf = new HashMap();
 * conf.put(Config.TOPOLOGY_WORKERS, 4);
 *
 * HeronSubmitter.submitTopology("mytopology", conf, builder.createTopology());
 * </pre>
 * <p>
 * Running the exact same topology in simulator (in process), and configuring it to log all tuples
 * emitted, looks like the following. Note that it lets the topology run for 10 seconds
 * before shutting down the local cluster.
 * <p>
 * <pre>
 * TopologyBuilder builder = new TopologyBuilder();
 *
 * builder.setSpout("1", new TestWordSpout(true), 5);
 * builder.setSpout("2", new TestWordSpout(true), 3);
 * builder.setBolt("3", new TestWordCounter(), 3)
 *          .fieldsGrouping("1", new Fields("word"))
 *          .fieldsGrouping("2", new Fields("word"));
 * builder.setBolt("4", new TestGlobalCount())
 *          .globalGrouping("1");
 *
 * Map conf = new HashMap();
 * conf.put(Config.TOPOLOGY_WORKERS, 4);
 * conf.put(Config.TOPOLOGY_DEBUG, true);
 *
 * LocalCluster cluster = new LocalCluster();
 * cluster.submitTopology("mytopology", conf, builder.createTopology());
 * Utils.sleep(10000);
 * cluster.shutdown();
 * </pre>
 * <p>
 * <p>The pattern for TopologyBuilder is to map component ids to components using the setSpout
 * and setBolt methods. Those methods return objects that are then used to declare
 * the inputs for that component.</p>
 */
public class TopologyBuilder {
  private Map<String, BoltDeclarer> bolts = new HashMap<String, BoltDeclarer>();
  private Map<String, SpoutDeclarer> spouts = new HashMap<String, SpoutDeclarer>();


  public HeronTopology createTopology() {
    TopologyAPI.Topology.Builder bldr = TopologyAPI.Topology.newBuilder();
    // First go thru the spouts
    for (Map.Entry<String, SpoutDeclarer> spout : spouts.entrySet()) {
      spout.getValue().dump(bldr);
    }
    // Then go thru the bolts
    for (Map.Entry<String, BoltDeclarer> bolt : bolts.entrySet()) {
      bolt.getValue().dump(bldr);
    }
    return new HeronTopology(bldr);
  }

  /**
   * Define a new bolt in this topology with parallelism of just one thread.
   *
   * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
   * @param bolt the bolt
   * @return use the returned object to declare the inputs to this component
   */
  public BoltDeclarer setBolt(String id, IRichBolt bolt) {
    return setBolt(id, bolt, null);
  }

  /**
   * Define a new bolt in this topology with the specified amount of parallelism.
   *
   * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
   * @param bolt the bolt
   * @param parallelismHint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somewhere around the cluster.
   * @return use the returned object to declare the inputs to this component
   */
  public BoltDeclarer setBolt(String id, IRichBolt bolt, Number parallelismHint) {
    validateComponentName(id);
    BoltDeclarer b = new BoltDeclarer(id, bolt, parallelismHint);
    bolts.put(id, b);
    return b;
  }

  /**
   * Define a new bolt in this topology. This defines a basic bolt, which is a
   * simpler to use but more restricted kind of bolt. Basic bolts are intended
   * for non-aggregation processing and automate the anchoring/acking process to
   * achieve proper reliability in the topology.
   *
   * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
   * @param bolt the basic bolt
   * @return use the returned object to declare the inputs to this component
   */
  public BoltDeclarer setBolt(String id, IBasicBolt bolt) {
    return setBolt(id, bolt, null);
  }

  /**
   * Define a new bolt in this topology. This defines a basic bolt, which is a
   * simpler to use but more restricted kind of bolt. Basic bolts are intended
   * for non-aggregation processing and automate the anchoring/acking process to
   * achieve proper reliability in the topology.
   *
   * @param id the id of this component. This id is referenced by other components that want to consume this bolt's outputs.
   * @param bolt the basic bolt
   * @param parallelismHint the number of tasks that should be assigned to execute this bolt. Each task will run on a thread in a process somwehere around the cluster.
   * @return use the returned object to declare the inputs to this component
   */
  public BoltDeclarer setBolt(String id, IBasicBolt bolt, Number parallelismHint) {
    return setBolt(id, new BasicBoltExecutor(bolt), parallelismHint);
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
   * @param id the id of this component. This id is referenced by other components that want to
   * consume this bolt's outputs.
   * @param bolt the windowed bolt
   * @param parallelismHint the number of tasks that should be assigned to execute this bolt.
   * Each task will run on a thread in a process somwehere around the cluster.
   * @return use the returned object to declare the inputs to this component
   * @throws IllegalArgumentException if {@code parallelismHint} is not positive
   */
  public BoltDeclarer setBolt(String id, IWindowedBolt bolt, Number parallelismHint) throws
      IllegalArgumentException {
    return setBolt(id, new WindowedBoltExecutor(bolt), parallelismHint);
  }

  /**
   * Define a new bolt in this topology. This defines a stateful windowed bolt, intended for stateful
   * windowing operations. The {@link IStatefulWindowedBolt#execute(TupleWindow)} method is triggered
   * for each window interval with the list of current events in the window. During initialization of
   * this bolt (potentially after failure) {@link IStatefulWindowedBolt#initState(State)}
   * is invoked with its previously saved state.
   * @param id the id of this component.
   * This id is referenced by other components that want to consume this bolt's outputs.
   * @param bolt the stateful windowed bolt
   * @param <K> Type of key for {@link org.apache.heron.api.state.HashMapState}
   * @param <V> Type of value for {@link org.apache.heron.api.state.HashMapState}
   * @return use the returned object to declare the inputs to this component
   * @throws IllegalArgumentException {@code parallelism_hint} is not positive
   */
  @SuppressWarnings("rawtypes")
  public <K extends Serializable, V extends Serializable> BoltDeclarer setBolt(
      String id, IStatefulWindowedBolt<K, V> bolt) throws
      IllegalArgumentException {
    return setBolt(id, bolt, null);
  }

  /**
   * Define a new bolt in this topology. This defines a stateful windowed bolt, intended for stateful
   * windowing operations. The {@link IStatefulWindowedBolt#execute(TupleWindow)} method is triggered
   * for each window interval with the list of current events in the window. During initialization of
   * this bolt (potentially after failure) {@link IStatefulWindowedBolt#initState(State)}
   * is invoked with its previously saved state.
   * @param id the id of this component.
   * This id is referenced by other components that want to consume this bolt's outputs.
   * @param bolt the stateful windowed bolt
   * @param parallelismHint the number of tasks that should be assigned to execute this bolt.
   * Each task will run on a thread in a process somwehere around the cluster.
   * @param <K> Type of key for {@link org.apache.heron.api.state.HashMapState}
   * @param <V> Type of value for {@link org.apache.heron.api.state.HashMapState}
   * @return use the returned object to declare the inputs to this component
   * @throws IllegalArgumentException {@code parallelism_hint} is not positive
   */
  @SuppressWarnings("rawtypes")
  public <K extends Serializable, V extends Serializable> BoltDeclarer setBolt(
      String id, IStatefulWindowedBolt<K, V> bolt, Number parallelismHint) throws
      IllegalArgumentException {
    return setBolt(id, new StatefulWindowedBoltExecutor<K, V>(bolt), parallelismHint);
  }

  /**
   * Define a new spout in this topology.
   *
   * @param id the id of this component. This id is referenced by other components that want to consume this spout's outputs.
   * @param spout the spout
   */
  public SpoutDeclarer setSpout(String id, IRichSpout spout) {
    return setSpout(id, spout, null);
  }

  /**
   * Define a new spout in this topology with the specified parallelism. If the spout declares
   * itself as non-distributed, the parallelismHint will be ignored and only one task
   * will be allocated to this component.
   *
   * @param id the id of this component. This id is referenced by other components that want to consume this spout's outputs.
   * @param parallelismHint the number of tasks that should be assigned to execute this spout. Each task will run on a thread in a process somwehere around the cluster.
   * @param spout the spout
   */
  public SpoutDeclarer setSpout(String id, IRichSpout spout, Number parallelismHint) {
    validateComponentName(id);
    SpoutDeclarer s = new SpoutDeclarer(id, spout, parallelismHint);
    spouts.put(id, s);
    return s;
  }

  private void validateComponentName(String name) {
    if (name.contains(",")) {
      throw new IllegalArgumentException("Component name should not contain comma(,)");
    }
    if (name.contains(":")) {
      throw new IllegalArgumentException("Component name should not contain colon(:)");
    }
    validateUnusedName(name);
  }

  private void validateUnusedName(String name) {
    if (bolts.containsKey(name)) {
      throw new IllegalArgumentException("Bolt has already been declared for name " + name);
    }
    if (spouts.containsKey(name)) {
      throw new IllegalArgumentException("Spout has already been declared for name " + name);
    }
  }
}

