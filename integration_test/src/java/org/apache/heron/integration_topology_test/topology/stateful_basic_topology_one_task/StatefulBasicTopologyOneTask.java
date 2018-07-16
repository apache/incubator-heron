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
package org.apache.heron.integration_topology_test.topology.stateful_basic_topology_one_task;

import java.net.MalformedURLException;

import org.apache.heron.api.tuple.Fields;
import org.apache.heron.integration_topology_test.common.AbstractTestTopology;
import org.apache.heron.integration_topology_test.common.bolt.StatefulIdentityBolt;
import org.apache.heron.integration_topology_test.common.spout.StatefulABSpout;
import org.apache.heron.integration_topology_test.core.TopologyTestTopologyBuilder;

public final class StatefulBasicTopologyOneTask extends AbstractTestTopology {

  private StatefulBasicTopologyOneTask(String[] args) throws MalformedURLException {
    super(args);
  }

  @Override
  protected TopologyTestTopologyBuilder buildStatefulTopology(TopologyTestTopologyBuilder builder) {
    builder.setSpout("stateful-ab-spout", new StatefulABSpout(true), 1);
    builder.setBolt("stateful-identity-bolt",
        new StatefulIdentityBolt(new Fields("word")), 3)
        .shuffleGrouping("stateful-ab-spout");
    return builder;
  }

  public static void main(String[] args) throws Exception {
    StatefulBasicTopologyOneTask topology = new StatefulBasicTopologyOneTask(args);
    topology.submit();
  }
}
