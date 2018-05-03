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

import java.util.ArrayList;
import java.util.List;

import org.apache.heron.api.grouping.CustomStreamGrouping;
import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.resource.TestBolt;

/**
 * Tests custom grouping by using round robin grouping from SPOUT to BOLT_A
 */
public class CustomGroupingTest extends AbstractTupleRoutingTest {

  @Override
  protected void initBoltA(TopologyBuilder topologyBuilder,
                           String boltId, String upstreamComponentId) {
    final CustomStreamGrouping myCustomGrouping =
        new MyRoundRobinCustomGrouping(getInitInfoKey(upstreamComponentId));

    topologyBuilder.setBolt(boltId, new TestBolt(), 1)
        .customGrouping(upstreamComponentId, myCustomGrouping);
  }

  @Override
  protected Component getComponentToVerify() {
    return Component.SPOUT;
  }

  @Override
  protected String getExpectedComponentInitInfo() {
    return "test-spout+test-spout+default+[1]";
  }

  private static final class MyRoundRobinCustomGrouping implements CustomStreamGrouping {
    private static final long serialVersionUID = -4141962710451507976L;
    private volatile int emitted = 0;
    private final String initInfoKey;

    private MyRoundRobinCustomGrouping(String initInfoKey) {
      super();
      this.initInfoKey = initInfoKey;
    }

    @Override
    public void prepare(TopologyContext context, String component,
                        String streamId, List<Integer> targetTasks) {

      ((StringBuilder) SingletonRegistry.INSTANCE.getSingleton(initInfoKey))
          .append(String.format("%s+%s+%s+%s",
              context.getThisComponentId(), component, streamId, targetTasks.toString()));
    }

    @Override
    public List<Integer> chooseTasks(List<Object> values) {
      List<Integer> res = new ArrayList<>();
      res.add(emitted);
      emitted++;
      return res;
    }
  }
}
