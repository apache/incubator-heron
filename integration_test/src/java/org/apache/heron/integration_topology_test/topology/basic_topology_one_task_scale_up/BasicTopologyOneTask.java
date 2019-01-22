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

package org.apache.heron.integration_topology_test.topology.basic_topology_one_task_scale_up;

import java.net.MalformedURLException;

import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.integration_test.common.bolt.IdentityBolt;
import org.apache.heron.integration_test.common.spout.ABSpout;
import org.apache.heron.integration_topology_test.common.AbstractTestTopology;

/**
 * Topology to test basic structure, single thread spout &amp; bolt, shuffleGrouping
 */
public final class BasicTopologyOneTask extends AbstractTestTopology {

  private BasicTopologyOneTask(String[] args) throws MalformedURLException {
    super(args);
  }

  @Override
  protected TopologyBuilder buildTopology(TopologyBuilder builder) {
    builder.setSpout("ab-spout", new ABSpout(true), 1);
    builder.setBolt("identity-bolt", new IdentityBolt(new Fields("word")), 2)
        .shuffleGrouping("ab-spout");
    return builder;
  }

  public static void main(String[] args) throws Exception {
    BasicTopologyOneTask topology = new BasicTopologyOneTask(args);
    topology.submit();
  }
}
