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

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.resource.TestSpout;

/**
 * Test spout that used emit direct to emit in a round robin pattern
 */
class EmitDirectRoundRobinSpout extends TestSpout {
  private static final Logger LOG = Logger.getLogger(EmitDirectRoundRobinSpout.class.getName());
  private static final long serialVersionUID = 5669629363927216006L;

  private final String initInfoKey;

  EmitDirectRoundRobinSpout(String initInfoKey) {
    super();
    this.initInfoKey = initInfoKey;
  }

  @Override
  public void open(Map<String, Object> map,
                   TopologyContext context,
                   SpoutOutputCollector spoutOutputCollector) {
    String componentId = context.getThisComponentId();
    String streamId = context.getThisStreams().iterator().next();
    Map<String, TopologyAPI.Grouping> targets = context.getThisTargets().get(streamId);

    List<Integer> targetTaskIds = context.getComponentTasks(targets.keySet().iterator().next());

    ((StringBuilder) SingletonRegistry.INSTANCE.getSingleton(this.initInfoKey))
        .append(String.format("%s+%s+%s+%s", componentId, componentId, streamId, targetTaskIds));
    super.open(map, context, spoutOutputCollector);
  }

  @Override
  protected void emit(SpoutOutputCollector collector, List<Object> tuple,
                      Object messageId, int emittedCount) {
    LOG.info(String.format("Emit direct tuple %s to %d", tuple, emittedCount));
    collector.emitDirect(emittedCount, tuple, messageId);
  }
}
