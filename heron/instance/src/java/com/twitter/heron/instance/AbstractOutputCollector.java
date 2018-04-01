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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.api.state.State;
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

  /**
   * The SuppressWarnings is only until TOPOLOGY_ENABLE_ACKING exists.
   * This warning will be removed once it is removed.
   */
  @SuppressWarnings("deprecation")
  public AbstractOutputCollector(IPluggableSerializer serializer,
                                 PhysicalPlanHelper helper,
                                 Communicator<Message> streamOutQueue,
                                 ComponentMetrics metrics) {
    this.serializer = serializer;
    this.metrics = metrics;
    this.totalTuplesEmitted = 0;
    this.helper = helper;

    Map<String, Object> config = helper.getTopologyContext().getTopologyConfig();
    if (config.containsKey(Config.TOPOLOGY_RELIABILITY_MODE)
        && config.get(Config.TOPOLOGY_RELIABILITY_MODE) != null) {
      this.ackEnabled =
     Config.TopologyReliabilityMode.valueOf(config.get(Config.TOPOLOGY_RELIABILITY_MODE).toString())
                        == Config.TopologyReliabilityMode.ATLEAST_ONCE;
    } else {
      // This is strictly for backwards compatiblity
      if (config.containsKey(Config.TOPOLOGY_ENABLE_ACKING)
          && config.get(Config.TOPOLOGY_ENABLE_ACKING) != null) {
        this.ackEnabled =
              Boolean.parseBoolean(config.get(Config.TOPOLOGY_ENABLE_ACKING).toString());
      } else {
        this.ackEnabled = false;
      }
    }

    this.outputter = new OutgoingTupleCollection(helper, streamOutQueue);
  }

  public void updatePhysicalPlanHelper(PhysicalPlanHelper physicalPlanHelper) {
    this.helper = physicalPlanHelper;
    this.outputter.updatePhysicalPlanHelper(physicalPlanHelper);
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

  // Flush the states
  public void sendOutState(State<Serializable, Serializable> state,
                           String checkpointId) {
    outputter.sendOutState(state, checkpointId);
  }

  // Clean the internal state of BoltOutputCollectorImpl
  public void clear() {
    outputter.clear();
  }

  public long getTotalTuplesEmitted() {
    return totalTuplesEmitted;
  }

  protected HeronTuples.HeronDataTuple.Builder initTupleBuilder(String streamId,
                                                                List<Object> tuple,
                                                                Integer emitDirectTaskId) {
    // Start construct the data tuple
    HeronTuples.HeronDataTuple.Builder builder = HeronTuples.HeronDataTuple.newBuilder();

    // set the key. This is mostly ignored
    builder.setKey(0);

    List<Integer> customGroupingTargetTaskIds = null;
    if (emitDirectTaskId != null) {
      // TODO: somehow assert that the input stream of the downstream bolt was configured
      // with directGrouping

      customGroupingTargetTaskIds = new ArrayList<>();
      customGroupingTargetTaskIds.add(emitDirectTaskId);
    } else if (!helper.isCustomGroupingEmpty()) {
      // customGroupingTargetTaskIds will be null if this stream is not CustomStreamGrouping
      customGroupingTargetTaskIds =
          helper.chooseTasksForCustomStreamGrouping(streamId, tuple);
    }

    if (customGroupingTargetTaskIds != null) {
      // It is a CustomStreamGrouping
      builder.addAllDestTaskIds(customGroupingTargetTaskIds);
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
