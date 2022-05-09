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

package org.apache.heron.simulator.instance;

import java.time.Duration;
import java.util.Map;

import com.google.protobuf.Message;

import org.apache.heron.api.Config;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.ExecutorLooper;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.basics.TypeUtils;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.utils.misc.PhysicalPlanHelper;
import org.apache.heron.instance.IInstance;

public class SpoutInstance
    extends org.apache.heron.instance.spout.SpoutInstance implements IInstance {

  private final boolean ackEnabled;
  private final int maxSpoutPending;
  private final Duration instanceEmitBatchTime;
  private final ByteAmount instanceEmitBatchSize;

  /**
   * The SuppressWarnings should go away once TOPOLOGY_ENABLE_ACKING is removed
   */
  @SuppressWarnings("deprecation")
  public SpoutInstance(PhysicalPlanHelper helper,
                       Communicator<Message> streamInQueue,
                       Communicator<Message> streamOutQueue, ExecutorLooper looper) {
    super(helper, streamInQueue, streamOutQueue, looper);
    Map<String, Object> config = helper.getTopologyContext().getTopologyConfig();
    SystemConfig systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    this.maxSpoutPending = TypeUtils.getInteger(config.get(Config.TOPOLOGY_MAX_SPOUT_PENDING));
    this.instanceEmitBatchTime = systemConfig.getInstanceEmitBatchTime();
    this.instanceEmitBatchSize = systemConfig.getInstanceEmitBatchSize();
    if (config.containsKey(Config.TOPOLOGY_RELIABILITY_MODE)) {
      this.ackEnabled = Config.TopologyReliabilityMode.ATLEAST_ONCE.equals(
                              config.get(Config.TOPOLOGY_RELIABILITY_MODE).toString());
    } else {
      // This is strictly for backwards compatibility
      this.ackEnabled = Boolean.parseBoolean((String) config.get(Config.TOPOLOGY_ENABLE_ACKING));
    }
  }

  @Override
  protected void produceTuple() {
    long totalTuplesEmitted = collector.getTotalTuplesEmitted();

    long totalDataEmittedInBytesBeforeCycle = collector.getTotalDataEmittedInBytes();

    long startOfCycle = System.nanoTime();

    while (!ackEnabled || (maxSpoutPending > collector.numInFlight())) {
      // Delegate to the use defined spout
      long startTime = System.nanoTime();
      spout.nextTuple();
      long latency = System.nanoTime() - startTime;
      spoutMetrics.nextTuple(latency);

      long newTotalTuplesEmitted = collector.getTotalTuplesEmitted();
      if (newTotalTuplesEmitted == totalTuplesEmitted) {
        // No tuples to emit....
        break;
      }

      totalTuplesEmitted = newTotalTuplesEmitted;

      // To avoid spending too much time
      if (System.nanoTime() - startOfCycle - instanceEmitBatchTime.toNanos() > 0) {
        break;
      }

      if (collector.getTotalDataEmittedInBytes() - totalDataEmittedInBytesBeforeCycle
          > instanceEmitBatchSize.asBytes()) {
        break;
      }
    }
  }
}
