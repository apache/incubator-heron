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

package org.apache.heron.grouping;

import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.resource.TestBolt;

/**
 * Tests emit direct of a spout to a bolt by using a round robin emit direct approach
 * from SPOUT to BOLT_A
 */
public class EmitDirectSpoutTest extends AbstractTupleRoutingTest {

  @Override
  protected void initSpout(TopologyBuilder topologyBuilder, String spoutId) {
    topologyBuilder.setSpout(spoutId, new EmitDirectRoundRobinSpout(getInitInfoKey(spoutId)), 1);
  }

  @Override
  protected void initBoltA(TopologyBuilder topologyBuilder,
                           String boltId, String upstreamComponentId) {
    topologyBuilder.setBolt(boltId, new TestBolt(), 1)
        .directGrouping(upstreamComponentId);
  }

  @Override
  protected Component getComponentToVerify() {
    return Component.SPOUT;
  }

  @Override
  protected String getExpectedComponentInitInfo() {
    return "test-spout+test-spout+default+[1]";
  }
}
