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

package com.twitter.heron.instance.bolt;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.bolt.IOutputCollector;
import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.api.tuple.Tuple;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.utils.metrics.BoltMetrics;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.common.utils.tuple.TupleImpl;
import com.twitter.heron.instance.AbstractOutputCollector;
import com.twitter.heron.proto.system.HeronTuples;

/**
 * BoltOutputCollectorImpl is used by bolt to emit tuples, it contains:
 * 1. IPluggableSerializer serializer, which will define the serializer
 * 2. OutgoingTupleCollection outputter.
 * When a tuple is to be emitted, it will serialize it and call OutgoingTupleCollection.admitBoltTuple()
 * to sent it out.
 * <p>
 * It will handle the extra work to emit a tuple:
 * For data tuples:
 * 1. Set the anchors for a tuple
 * 2. Pack the tuple and submit the OutgoingTupleCollection's addDataTuple
 * 3. Update the metrics
 * <p>
 * For Control tuples (ack &amp; fail):
 * 1. Set the anchors for a tuple
 * 2. Pack the tuple and submit the OutgoingTupleCollection's addDataTuple
 * 3. Update the metrics
 */
public class BoltOutputCollectorImpl extends AbstractOutputCollector implements IOutputCollector {
  private static final Logger LOG = Logger.getLogger(BoltOutputCollectorImpl.class.getName());

  // Reference to update the bolt metrics
  private final BoltMetrics boltMetrics;

  protected BoltOutputCollectorImpl(IPluggableSerializer serializer,
                                    PhysicalPlanHelper helper,
                                    Communicator<HeronTuples.HeronTupleSet> streamOutQueue,
                                    BoltMetrics boltMetrics) {
    super(serializer, helper, streamOutQueue, boltMetrics);
    this.boltMetrics = boltMetrics;

    if (helper.getMyBolt() == null) {
      throw new RuntimeException(helper.getMyTaskId() + " is not a bolt ");
    }
  }

  @Override
  public List<Integer> emit(String streamId, Collection<Tuple> anchors, List<Object> tuple) {
    return admitBoltTuple(streamId, anchors, tuple);
  }

  @Override
  public void emitDirect(int taskId, String streamId,
                         Collection<Tuple> anchors, List<Object> tuple) {
    throw new RuntimeException("emitDirect not supported");
  }

  @Override
  public void reportError(Throwable error) {
    LOG.log(Level.SEVERE, "Reporting an error in topology code ", error);
  }

  @Override
  public void ack(Tuple input) {
    admitAckTuple(input);
  }

  @Override
  public void fail(Tuple input) {
    admitFailTuple(input);
  }

  /////////////////////////////////////////////////////////
  // Following private methods are internal implementations
  /////////////////////////////////////////////////////////
  private List<Integer> admitBoltTuple(String streamId,
                                       Collection<Tuple> anchors,
                                       List<Object> tuple) {
    if (getPhysicalPlanHelper().isTerminatedComponent()) {
      // No need to handle this tuples
      return null;
    }

    // Start construct the data tuple
    HeronTuples.HeronDataTuple.Builder bldr = initTupleBuilder(streamId, tuple);

    // Set the anchors for a tuple
    if (anchors != null) {
      // This message is rooted
      Set<HeronTuples.RootId> mergedRoots = new HashSet<>();
      for (Tuple tpl : anchors) {
        if (tpl instanceof TupleImpl) {
          TupleImpl t = (TupleImpl) tpl;
          mergedRoots.addAll(t.getRoots());
        }
      }
      for (HeronTuples.RootId rt : mergedRoots) {
        bldr.addRoots(rt);
      }
    }

    sendTuple(bldr, streamId, tuple);

    // TODO:- remove this after changing the api
    return null;
  }

  private void admitAckTuple(Tuple tuple) {
    Duration latency = Duration.ZERO;
    if (ackEnabled) {
      if (tuple instanceof TupleImpl) {
        TupleImpl tuplImpl = (TupleImpl) tuple;

        HeronTuples.AckTuple.Builder bldr = HeronTuples.AckTuple.newBuilder();
        bldr.setAckedtuple(tuplImpl.getTupleKey());

        long tupleSizeInBytes = 0;

        for (HeronTuples.RootId rt : tuplImpl.getRoots()) {
          bldr.addRoots(rt);
          tupleSizeInBytes += rt.getSerializedSize();
        }
        outputter.addAckTuple(bldr, tupleSizeInBytes);

        latency = Duration.ofNanos(System.nanoTime()).minusNanos(tuplImpl.getCreationTime());
      }
    }

    // Invoke user-defined boltAck task hook
    getPhysicalPlanHelper().getTopologyContext().invokeHookBoltAck(tuple, latency);

    boltMetrics.ackedTuple(
        tuple.getSourceStreamId(), tuple.getSourceComponent(), latency.toNanos());
  }

  private void admitFailTuple(Tuple tuple) {
    Duration latency = Duration.ZERO;
    if (ackEnabled) {
      if (tuple instanceof TupleImpl) {
        TupleImpl tuplImpl = (TupleImpl) tuple;

        HeronTuples.AckTuple.Builder bldr = HeronTuples.AckTuple.newBuilder();
        bldr.setAckedtuple(tuplImpl.getTupleKey());

        long tupleSizeInBytes = 0;

        for (HeronTuples.RootId rt : tuplImpl.getRoots()) {
          bldr.addRoots(rt);
          tupleSizeInBytes += rt.getSerializedSize();
        }
        outputter.addFailTuple(bldr, tupleSizeInBytes);

        latency = Duration.ofNanos(System.nanoTime()).minusNanos(tuplImpl.getCreationTime());
      }
    }

    // Invoke user-defined boltFail task hook
    getPhysicalPlanHelper().getTopologyContext().invokeHookBoltFail(tuple, latency);

    boltMetrics.failedTuple(
        tuple.getSourceStreamId(), tuple.getSourceComponent(), latency.toNanos());
  }
}
