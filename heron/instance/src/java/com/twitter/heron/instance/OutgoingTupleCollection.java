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

package com.twitter.heron.instance;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.proto.system.HeronTuples;

/**
 * Implements OutgoingTupleCollection will be able to handle some basic methods for send out tuples
 * 1. initNewControlTuple or initNewDataTuple
 * 2. addDataTuple, addAckTuple and addFailTuple
 * 3. flushRemaining tuples and sent out the tuples
 * <p>
 * In fact, when talking about to send out tuples, we mean we push them to the out queues.
 */
public class OutgoingTupleCollection {
  protected final PhysicalPlanHelper helper;
  // We have just one outQueue responsible for both control tuples and data tuples
  private final Communicator<HeronTuples.HeronTupleSet> outQueue;
  private final SystemConfig systemConfig;

  private HeronTuples.HeronDataTupleSet.Builder currentDataTuple;
  private HeronTuples.HeronControlTupleSet.Builder currentControlTuple;

  // Total data emitted in bytes for the entire life
  private long totalDataEmittedInBytes = 0;

  // Current size in bytes for data types to pack into the HeronTupleSet
  private long currentDataTupleSizeInBytes = 0;
  // Maximum data tuple size in bytes we can put in one HeronTupleSet
  private long maxDataTupleSizeInBytes = Long.MAX_VALUE;

  private int dataTupleSetCapacity = Integer.MAX_VALUE;
  private int controlTupleSetCapacity = Integer.MAX_VALUE;

  public OutgoingTupleCollection(
      PhysicalPlanHelper helper,
      Communicator<HeronTuples.HeronTupleSet> outQueue) {
    this.outQueue = outQueue;
    this.helper = helper;
    this.systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    // Read the config values
    this.dataTupleSetCapacity = systemConfig.getInstanceSetDataTupleCapacity();
    this.maxDataTupleSizeInBytes = systemConfig.getInstanceSetDataTupleSizeBytes();
    this.controlTupleSetCapacity = systemConfig.getInstanceSetControlTupleCapacity();
  }

  public void sendOutTuples() {
    flushRemaining();
  }

  public void addDataTuple(
      String streamId,
      HeronTuples.HeronDataTuple.Builder newTuple,
      long tupleSizeInBytes) {
    if (currentDataTuple == null
        || !currentDataTuple.getStream().getId().equals(streamId)
        || currentDataTuple.getTuplesCount() > dataTupleSetCapacity
        || currentDataTupleSizeInBytes > maxDataTupleSizeInBytes) {
      initNewDataTuple(streamId);
    }
    currentDataTuple.addTuples(newTuple);

    currentDataTupleSizeInBytes += tupleSizeInBytes;
    totalDataEmittedInBytes += tupleSizeInBytes;
  }

  public void addAckTuple(HeronTuples.AckTuple.Builder newTuple, long tupleSizeInBytes) {
    if (currentControlTuple == null
        || currentControlTuple.getFailsCount() > 0
        || currentControlTuple.getAcksCount() > controlTupleSetCapacity) {
      initNewControlTuple();
    }
    currentControlTuple.addAcks(newTuple);

    // Add the size of data in bytes ready to send out
    totalDataEmittedInBytes += tupleSizeInBytes;
  }

  public void addFailTuple(HeronTuples.AckTuple.Builder newTuple, long tupleSizeInBytes) {
    if (currentControlTuple == null
        || currentControlTuple.getAcksCount() > 0
        || currentControlTuple.getFailsCount() > controlTupleSetCapacity) {
      initNewControlTuple();
    }
    currentControlTuple.addFails(newTuple);

    // Add the size of data in bytes ready to send out
    totalDataEmittedInBytes += tupleSizeInBytes;
  }

  private void initNewDataTuple(String streamId) {
    flushRemaining();

    // Reset the set for data tuple
    currentDataTupleSizeInBytes = 0;

    TopologyAPI.StreamId.Builder sbldr = TopologyAPI.StreamId.newBuilder();
    sbldr.setId(streamId);
    sbldr.setComponentName(helper.getMyComponent());
    currentDataTuple = HeronTuples.HeronDataTupleSet.newBuilder();
    currentDataTuple.setStream(sbldr);
  }

  private void initNewControlTuple() {
    flushRemaining();
    currentControlTuple = HeronTuples.HeronControlTupleSet.newBuilder();
  }

  private void flushRemaining() {
    if (currentDataTuple != null) {
      HeronTuples.HeronTupleSet.Builder bldr = HeronTuples.HeronTupleSet.newBuilder();
      bldr.setData(currentDataTuple);

      pushTupleToQueue(bldr, outQueue);

      currentDataTuple = null;
    }
    if (currentControlTuple != null) {
      HeronTuples.HeronTupleSet.Builder bldr = HeronTuples.HeronTupleSet.newBuilder();
      bldr.setControl(currentControlTuple);
      pushTupleToQueue(bldr, outQueue);

      currentControlTuple = null;
    }
  }

  private void pushTupleToQueue(HeronTuples.HeronTupleSet.Builder bldr,
                                Communicator<HeronTuples.HeronTupleSet> out) {
    // The Communicator has un-bounded capacity so the offer will always be successful
    out.offer(bldr.build());
  }

  // Return true we could offer item to outQueue
  public boolean isOutQueuesAvailable() {
    return outQueue.size() < outQueue.getExpectedAvailableCapacity();
  }

  public long getTotalDataEmittedInBytes() {
    return totalDataEmittedInBytes;
  }

  // Clean the internal state of OutgoingTupleCollection
  public void clear() {
    currentControlTuple = null;
    currentDataTuple = null;

    outQueue.clear();
  }
}
