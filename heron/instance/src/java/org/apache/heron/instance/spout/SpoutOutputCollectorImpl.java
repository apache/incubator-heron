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

package org.apache.heron.instance.spout;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Queue;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.apache.heron.api.serializer.IPluggableSerializer;
import org.apache.heron.api.spout.ISpoutOutputCollector;
import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.utils.metrics.ComponentMetrics;
import org.apache.heron.common.utils.misc.PhysicalPlanHelper;
import org.apache.heron.common.utils.misc.TupleKeyGenerator;
import org.apache.heron.instance.AbstractOutputCollector;
import org.apache.heron.proto.system.HeronTuples;

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
public class SpoutOutputCollectorImpl
    extends AbstractOutputCollector implements ISpoutOutputCollector {
  private static final Logger LOG = Logger.getLogger(SpoutOutputCollectorImpl.class.getName());

  // Map from tuple key to composite object with insertion-order, i.e. ordered by time
  private final LinkedHashMap<Long, RootTupleInfo> inFlightTuples;

  private final TupleKeyGenerator keyGenerator;

  // When acking is not enabled, if the spout does an emit with a anchor
  // we need to ack it immediately. This keeps the list of those
  private final Queue<RootTupleInfo> immediateAcks;

  protected SpoutOutputCollectorImpl(IPluggableSerializer serializer,
                                     PhysicalPlanHelper helper,
                                     Communicator<Message> streamOutQueue,
                                     ComponentMetrics spoutMetrics) {
    super(serializer, helper, streamOutQueue, spoutMetrics);
    if (helper.getMySpout() == null) {
      throw new RuntimeException(helper.getMyTaskId() + " is not a spout ");
    }

    this.keyGenerator = new TupleKeyGenerator();

    // with default capacity, load factor and insertion order
    inFlightTuples = new LinkedHashMap<>();

    if (!ackEnabled) {
      immediateAcks = new ArrayDeque<>();
    } else {
      immediateAcks = null;
    }
  }

  @Override
  public List<Integer> emit(String streamId, List<Object> tuple, Object messageId) {
    return admitSpoutTuple(streamId, tuple, messageId, null);
  }

  @Override
  public void emitDirect(int taskId, String streamId, List<Object> tuple, Object messageId) {
    admitSpoutTuple(streamId, tuple, messageId, taskId);
  }

  // Log the report error and also send the stack trace to metrics manager.
  @Override
  public void reportError(Throwable error) {
    LOG.log(Level.SEVERE, "Reporting an error in topology code ", error);
  }

  public boolean isAckEnabled() {
    return ackEnabled;
  }


  /////////////////////////////////////////////////////////
  // Following public methods are used for querying or
  // interacting internal state of the BoltOutputCollectorImpl
  /////////////////////////////////////////////////////////
  public int numInFlight() {
    return inFlightTuples.size();
  }

  Queue<RootTupleInfo> getImmediateAcks() {
    return immediateAcks;
  }

  RootTupleInfo retireInFlight(long rootId) {
    return inFlightTuples.remove(rootId);
  }

  List<RootTupleInfo> retireExpired(Duration timeout) {
    List<RootTupleInfo> retval = new ArrayList<>();
    long curTime = System.nanoTime();

    // The LinkedHashMap is ordered by insertion order, i.e. ordered by time
    // So we want need to iterate from the start and remove all items until
    // we meet the RootTupleInfo no need to expire
    Iterator<RootTupleInfo> iterator = inFlightTuples.values().iterator();
    while (iterator.hasNext()) {
      RootTupleInfo rootTupleInfo = iterator.next();
      if (rootTupleInfo.isExpired(curTime, timeout.toNanos())) {
        retval.add(rootTupleInfo);
        iterator.remove();
      } else {
        break;
      }
    }

    return retval;
  }

  /////////////////////////////////////////////////////////
  // Following private methods are internal implementations
  /////////////////////////////////////////////////////////

  private List<Integer> admitSpoutTuple(String streamId, List<Object> tuple,
                                        Object messageId, Integer emitDirectTaskId) {
    // No need to send tuples if it is already terminated
    if (getPhysicalPlanHelper().isTerminatedComponent()) {
      return null;
    }

    // Start construct the data tuple
    HeronTuples.HeronDataTuple.Builder bldr = initTupleBuilder(streamId, tuple, emitDirectTaskId);

    if (messageId != null) {
      RootTupleInfo tupleInfo = new RootTupleInfo(streamId, messageId);
      if (ackEnabled) {
        // This message is rooted
        HeronTuples.RootId.Builder rtbldr = establishRootId(tupleInfo);
        bldr.addRoots(rtbldr);
      } else {
        immediateAcks.offer(tupleInfo);
      }
    }

    sendTuple(bldr, streamId, tuple);

    // TODO:- remove this after changing the API
    return null;
  }

  private HeronTuples.RootId.Builder establishRootId(RootTupleInfo tupleInfo) {
    // This message is rooted
    long rootId = keyGenerator.next();
    HeronTuples.RootId.Builder rtbldr = HeronTuples.RootId.newBuilder();
    rtbldr.setTaskid(getPhysicalPlanHelper().getMyTaskId());
    rtbldr.setKey(rootId);
    inFlightTuples.put(rootId, tupleInfo);
    return rtbldr;
  }
}
