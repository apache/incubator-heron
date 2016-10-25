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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.api.spout.ISpoutOutputCollector;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.utils.metrics.SpoutMetrics;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.common.utils.misc.TupleKeyGenerator;
import com.twitter.heron.proto.system.HeronTuples;

/**
 * SpoutOutputCollectorImpl is used by bolt to emit tuples, it contains:
 * 1. IPluggableSerializer serializer, which will define the serializer
 * 2. OutgoingTupleCollection outputter.
 * When a tuple is to be emitted, it will serialize it and call OutgoingTupleCollection.admitSpoutTuple()
 * to sent it out.
 * <p>
 * It will only emit data tuples; it will not send control tuples (ack&amp;fail)
 * 1. Whether some tuples are expired; should be considered as failed automatically
 * 2. The pending tuples to be acked
 * 3. Maintain some statistics, for instance, total tuples emitted.
 * <p>
 */
public class SpoutOutputCollectorImpl implements ISpoutOutputCollector {
  private static final Logger LOG = Logger.getLogger(SpoutOutputCollectorImpl.class.getName());

  // Map from tuple key to composite object with insertion-order, i.e. ordered by time
  private final LinkedHashMap<Long, RootTupleInfo> inFlightTuples;

  private final TupleKeyGenerator keyGenerator;

  private final SpoutMetrics spoutMetrics;
  private PhysicalPlanHelper helper;

  private final boolean ackingEnabled;
  // When acking is not enabled, if the spout does an emit with a anchor
  // we need to ack it immediately. This keeps the list of those
  private final Queue<RootTupleInfo> immediateAcks;

  private final IPluggableSerializer serializer;
  private final OutgoingTupleCollection outputter;

  private long totalTuplesEmitted;

  public SpoutOutputCollectorImpl(IPluggableSerializer serializer,
                                  PhysicalPlanHelper helper,
                                  Communicator<HeronTuples.HeronTupleSet> streamOutQueue,
                                  SpoutMetrics spoutMetrics) {
    if (helper.getMySpout() == null) {
      throw new RuntimeException(helper.getMyTaskId() + " is not a spout ");
    }

    this.serializer = serializer;
    this.helper = helper;
    this.spoutMetrics = spoutMetrics;
    this.keyGenerator = new TupleKeyGenerator();
    updatePhysicalPlanHelper(helper);

    // with default capacity, load factor and insertion order
    inFlightTuples = new LinkedHashMap<Long, RootTupleInfo>();

    Map<String, Object> config = helper.getTopologyContext().getTopologyConfig();
    if (config.containsKey(Config.TOPOLOGY_ENABLE_ACKING)
        && config.get(Config.TOPOLOGY_ENABLE_ACKING) != null) {
      this.ackingEnabled =
          Boolean.parseBoolean(config.get(Config.TOPOLOGY_ENABLE_ACKING).toString());
    } else {
      this.ackingEnabled = false;
    }

    if (!ackingEnabled) {
      immediateAcks = new ArrayDeque<RootTupleInfo>();
    } else {
      immediateAcks = null;
    }

    this.outputter = new OutgoingTupleCollection(helper.getMyComponent(), streamOutQueue);
  }

  public void updatePhysicalPlanHelper(PhysicalPlanHelper physicalPlanHelper) {
    this.helper = physicalPlanHelper;
  }

  /////////////////////////////////////////////////////////
  // Following public methods are overrides OutputCollector
  /////////////////////////////////////////////////////////

  @Override
  public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
    return admitSpoutTuple(streamId, tuple, messageId);
  }

  @Override
  public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
    throw new RuntimeException("emitDirect Not implemented");
  }

  // Log the report error and also send the stack trace to metrics manager.
  @Override
  public void reportError(Throwable error) {
    LOG.log(Level.SEVERE, "Reporting an error in topology code ", error);
  }


  /////////////////////////////////////////////////////////
  // Following public methods are used for querying or
  // interacting internal state of the BoltOutputCollectorImpl
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

  public long getTotalTuplesEmitted() {
    return totalTuplesEmitted;
  }

  public int numInFlight() {
    return inFlightTuples.size();
  }

  public Queue<RootTupleInfo> getImmediateAcks() {
    return immediateAcks;
  }

  public RootTupleInfo retireInFlight(long rootId) {
    return inFlightTuples.remove(rootId);
  }

  public List<RootTupleInfo> retireExpired(long timeout) {
    List<RootTupleInfo> retval = new ArrayList<RootTupleInfo>();
    long curTime = System.nanoTime();

    // The LinkedHashMap is ordered by insertion order, i.e. ordered by time
    // So we want need to iterate from the start and remove all items until
    // we meet the RootTupleInfo no need to expire
    Iterator<RootTupleInfo> iterator = inFlightTuples.values().iterator();
    while (iterator.hasNext()) {
      RootTupleInfo rootTupleInfo = iterator.next();
      if (rootTupleInfo.isExpired(curTime, timeout)) {
        retval.add(rootTupleInfo);
        iterator.remove();
      } else {
        break;
      }
    }

    return retval;
  }

  // Clean the internal state of BoltOutputCollectorImpl
  public void clear() {
    outputter.clear();
  }

  /////////////////////////////////////////////////////////
  // Following private methods are internal implementations
  /////////////////////////////////////////////////////////

  private List<Integer> admitSpoutTuple(String streamId, List<Object> tuple, Object messageId) {
    // First check whether this tuple is sane
    helper.checkOutputSchema(streamId, tuple);

    // customGroupingTargetTaskIds will be null if this stream is not CustomStreamGrouping
    List<Integer> customGroupingTargetTaskIds =
        helper.chooseTasksForCustomStreamGrouping(streamId, tuple);

    // Invoke user-defined emit task hook
    helper.getTopologyContext().invokeHookEmit(tuple, streamId, customGroupingTargetTaskIds);

    // Start construct the data tuple
    HeronTuples.HeronDataTuple.Builder bldr = HeronTuples.HeronDataTuple.newBuilder();

    // set the key. This is mostly ignored
    bldr.setKey(0);

    if (customGroupingTargetTaskIds != null) {
      // It is a CustomStreamGrouping
      for (Integer taskId : customGroupingTargetTaskIds) {
        bldr.addDestTaskIds(taskId);
      }
    }

    if (messageId != null) {
      RootTupleInfo tupleInfo = new RootTupleInfo(streamId, messageId);
      if (ackingEnabled) {
        // This message is rooted
        HeronTuples.RootId.Builder rtbldr = EstablishRootId(tupleInfo);
        bldr.addRoots(rtbldr);
      } else {
        immediateAcks.offer(tupleInfo);
      }
    }

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
    spoutMetrics.serializeDataTuple(streamId, latency);

    // submit to outputter
    outputter.addDataTuple(streamId, bldr, tupleSizeInBytes);
    totalTuplesEmitted++;
    spoutMetrics.emittedTuple(streamId);

    // TODO:- remove this after changing the api
    return null;
  }

  private HeronTuples.RootId.Builder EstablishRootId(RootTupleInfo tupleInfo) {
    // This message is rooted
    long rootId = keyGenerator.next();
    HeronTuples.RootId.Builder rtbldr = HeronTuples.RootId.newBuilder();
    rtbldr.setTaskid(helper.getMyTaskId());
    rtbldr.setKey(rootId);
    inFlightTuples.put(rootId, tupleInfo);
    return rtbldr;
  }
}
