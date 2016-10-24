// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.simulator.instance;

import java.util.Map;

import com.twitter.heron.api.Config;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.Constants;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.basics.TypeUtils;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.proto.system.HeronTuples;

public class SpoutInstance
    extends com.twitter.heron.instance.spout.SpoutInstance implements IInstance {

  private final boolean ackEnabled;
  private final int maxSpoutPending;
  private final long instanceEmitBatchTime;
  private final long instanceEmitBatchSize;

  public SpoutInstance(PhysicalPlanHelper helper,
                       Communicator<HeronTuples.HeronTupleSet> streamInQueue,
                       Communicator<HeronTuples.HeronTupleSet> streamOutQueue, SlaveLooper looper) {
    super(helper, streamInQueue, streamOutQueue, looper);
    Map<String, Object> config = helper.getTopologyContext().getTopologyConfig();
    SystemConfig systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    this.maxSpoutPending = TypeUtils.getInteger(config.get(Config.TOPOLOGY_MAX_SPOUT_PENDING));
    this.instanceEmitBatchTime =
        systemConfig.getInstanceEmitBatchTimeMs() * Constants.MILLISECONDS_TO_NANOSECONDS;
    this.instanceEmitBatchSize = systemConfig.getInstanceEmitBatchSizeBytes();
    this.ackEnabled = Boolean.parseBoolean((String) config.get(Config.TOPOLOGY_ENABLE_ACKING));
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
      if (System.nanoTime() - startOfCycle - instanceEmitBatchTime > 0) {
        break;
      }

      if (collector.getTotalDataEmittedInBytes() - totalDataEmittedInBytesBeforeCycle
          > instanceEmitBatchSize) {
        break;
      }
    }
  }
}
