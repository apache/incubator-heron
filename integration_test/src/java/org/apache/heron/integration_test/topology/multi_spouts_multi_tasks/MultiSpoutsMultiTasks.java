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

package org.apache.heron.integration_test.topology.multi_spouts_multi_tasks;

import java.net.MalformedURLException;

import org.apache.heron.api.tuple.Fields;
import org.apache.heron.integration_test.common.AbstractTestTopology;
import org.apache.heron.integration_test.common.bolt.IdentityBolt;
import org.apache.heron.integration_test.common.spout.ABSpout;
import org.apache.heron.integration_test.core.TestTopologyBuilder;

/**
 * Topology to test Spout with multiple threads
 */
public final class MultiSpoutsMultiTasks extends AbstractTestTopology {

  private MultiSpoutsMultiTasks(String[] args) throws MalformedURLException {
    super(args);
  }

  @Override
  protected TestTopologyBuilder buildTopology(TestTopologyBuilder builder) {
    builder.setSpout("ab-spout-1", new ABSpout(), 3);
    builder.setSpout("ab-spout-2", new ABSpout(), 3);
    builder.setBolt("identity-bolt", new IdentityBolt(new Fields("word")), 1)
        .shuffleGrouping("ab-spout-1")
        .shuffleGrouping("ab-spout-2");
    return builder;
  }

  public static void main(String[] args) throws Exception {
    MultiSpoutsMultiTasks topology = new MultiSpoutsMultiTasks(args);
    topology.submit();
  }
}
