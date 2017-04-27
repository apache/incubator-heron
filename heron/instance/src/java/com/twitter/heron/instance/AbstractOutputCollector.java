//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.instance;

import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.utils.metrics.ComponentMetrics;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.proto.system.HeronTuples;

/**
 * Common functionality used by both bolt and spout output collectors
 */
public class AbstractOutputCollector {
  protected final IPluggableSerializer serializer;
  protected final OutgoingTupleCollection outputter;
  protected final ComponentMetrics metrics;
  protected final boolean ackEnabled;
  private long totalTuplesEmitted;
  private PhysicalPlanHelper helper;

  public AbstractOutputCollector(IPluggableSerializer serializer,
                                 PhysicalPlanHelper helper,
                                 Communicator<HeronTuples.HeronTupleSet> streamOutQueue,
                                 ComponentMetrics metrics) {
    this.serializer = serializer;
    this.metrics = metrics;
    this.totalTuplesEmitted = 0;
    updatePhysicalPlanHelper(helper);

    Map<String, Object> config = helper.getTopologyContext().getTopologyConfig();
    if (config.containsKey(Config.TOPOLOGY_ENABLE_ACKING)
        && config.get(Config.TOPOLOGY_ENABLE_ACKING) != null) {
      this.ackEnabled = Boolean.parseBoolean(config.get(Config.TOPOLOGY_ENABLE_ACKING).toString());
    } else {
      this.ackEnabled = false;
    }

    this.outputter = new OutgoingTupleCollection(helper.getMyComponent(), streamOutQueue);
  }

  public void updatePhysicalPlanHelper(PhysicalPlanHelper physicalPlanHelper) {
    this.helper = physicalPlanHelper;
  }

  public PhysicalPlanHelper getPhysicalPlanHelper() {
    return helper;
  }
  /////////////////////////////////////////////////////////
  // Following public methods are used for querying or
  // interacting internal state of the output collectors
  /////////////////////////////////////////////////////////

  // Return true we could offer item to outQueue
  public boolean isOutQueuesAvailable() {
    return outputter.isOutQueuesAvailable();
  }

  // Return the total data emitted in bytes
  public long getTotalDataEmittedInBytes() {
    return outputter.getTotalDataEmittedInBytes();
  }

  // Flush the tuples to next stage
  public void sendOutTuples() {
    outputter.sendOutTuples();
  }

  // Clean the internal state of BoltOutputCollectorImpl
  public void clear() {
    outputter.clear();
  }

  public long getTotalTuplesEmitted() {
    return totalTuplesEmitted;
  }

  protected HeronTuples.HeronDataTuple.Builder initTupleBuilder(String streamId,
                                                                List<Object> tuple) {
    // Start construct the data tuple
    HeronTuples.HeronDataTuple.Builder builder = HeronTuples.HeronDataTuple.newBuilder();

    // set the key. This is mostly ignored
    builder.setKey(0);

    List<Integer> customGroupingTargetTaskIds = null;
    if (!helper.isCustomGroupingEmpty()) {
      // customGroupingTargetTaskIds will be null if this stream is not CustomStreamGrouping
      customGroupingTargetTaskIds =
          helper.chooseTasksForCustomStreamGrouping(streamId, tuple);

      if (customGroupingTargetTaskIds != null) {
        // It is a CustomStreamGrouping
        builder.addAllDestTaskIds(customGroupingTargetTaskIds);
      }
    }

    // Invoke user-defined emit task hook
    helper.getTopologyContext().invokeHookEmit(tuple, streamId, customGroupingTargetTaskIds);

    return builder;
  }

  protected void sendTuple(HeronTuples.HeronDataTuple.Builder bldr,
                           String streamId, List<Object> tuple) {
    long tupleSizeInBytes = 0;
    long startTime = System.nanoTime();

    // Serialize it
    for (Object obj : tuple) {
      byte[] b = serializer.serialize(obj);
      ByteString bstr = ByteString.copyFrom(b);
      bldr.addValues(bstr);
      tupleSizeInBytes += b.length;
    }

    long latency = System.nanoTime() - startTime;
    metrics.serializeDataTuple(streamId, latency);
    // submit to outputter
    outputter.addDataTuple(streamId, bldr, tupleSizeInBytes);
    totalTuplesEmitted++;

    // Update metrics
    metrics.emittedTuple(streamId);
  }
}
