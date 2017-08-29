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

import java.io.Serializable;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.api.state.State;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.common.utils.misc.SerializeDeSerializeHelper;
import com.twitter.heron.proto.ckptmgr.CheckpointManager;
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
  protected PhysicalPlanHelper helper;
  // We have just one outQueue responsible for both control tuples and data tuples
  private final Communicator<Message> outQueue;

  // Maximum data tuple size in bytes we can put in one HeronTupleSet
  private final ByteAmount maxDataTupleSize;
  private final int dataTupleSetCapacity;
  private final int controlTupleSetCapacity;

  private final IPluggableSerializer serializer;

  private HeronTuples.HeronDataTupleSet.Builder currentDataTuple;
  private HeronTuples.HeronControlTupleSet.Builder currentControlTuple;

  // Total data emitted in bytes for the entire life
  private long totalDataEmittedInBytes;

  // Current size in bytes for data types to pack into the HeronTupleSet
  private long currentDataTupleSizeInBytes;

  public OutgoingTupleCollection(
      PhysicalPlanHelper helper,
      Communicator<Message> outQueue) {
    this.outQueue = outQueue;
    this.helper = helper;
    SystemConfig systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    this.serializer =
        SerializeDeSerializeHelper.getSerializer(helper.getTopologyContext().getTopologyConfig());

    // Initialize the values in constructor
    this.totalDataEmittedInBytes = 0;
    this.currentDataTupleSizeInBytes = 0;

    // Read the config values
    this.dataTupleSetCapacity = systemConfig.getInstanceSetDataTupleCapacity();
    this.maxDataTupleSize = systemConfig.getInstanceSetDataTupleSize();
    this.controlTupleSetCapacity = systemConfig.getInstanceSetControlTupleCapacity();
  }

  public void sendOutTuples() {
    flushRemaining();
  }

  /**
   * Send out the instance's state with corresponding checkpointId
   * @param state instance's state
   * @param checkpointId the checkpointId
   */
  public void sendOutState(State<Serializable, Serializable> state,
                           String checkpointId) {
    // flush all the current data before sending the state
    flushRemaining();

    // Serialize the state
    byte[] serializedState = serializer.serialize(state);

    // Construct the instance state checkpoint
    CheckpointManager.InstanceStateCheckpoint instanceState =
        CheckpointManager.InstanceStateCheckpoint.newBuilder()
          .setCheckpointId(checkpointId)
          .setState(ByteString.copyFrom(serializedState))
          .build();

    CheckpointManager.StoreInstanceStateCheckpoint storeRequest =
        CheckpointManager.StoreInstanceStateCheckpoint.newBuilder()
            .setState(instanceState)
            .build();

    // Put the checkpoint to out stream queue
    outQueue.offer(storeRequest);
  }

  public void addDataTuple(
      String streamId,
      HeronTuples.HeronDataTuple.Builder newTuple,
      long tupleSizeInBytes) {
    if (tupleSizeInBytes > maxDataTupleSize.asBytes()) {
      throw new RuntimeException(
          String.format("Data tuple (stream id: %s) is too large: %d bytes", streamId,
              tupleSizeInBytes));
    }
    if (currentDataTuple == null
        || !currentDataTuple.getStream().getId().equals(streamId)
        || currentDataTuple.getTuplesCount() >= dataTupleSetCapacity
        || currentDataTupleSizeInBytes >= maxDataTupleSize.asBytes()) {
      initNewDataTuple(streamId);
    }
    currentDataTuple.addTuples(newTuple);

    currentDataTupleSizeInBytes += tupleSizeInBytes;
    totalDataEmittedInBytes += tupleSizeInBytes;
  }

  public void addAckTuple(HeronTuples.AckTuple.Builder newTuple, long tupleSizeInBytes) {
    if (currentControlTuple == null
        || currentControlTuple.getFailsCount() > 0
        || currentControlTuple.getAcksCount() >= controlTupleSetCapacity) {
      initNewControlTuple();
    }
    currentControlTuple.addAcks(newTuple);

    // Add the size of data in bytes ready to send out
    totalDataEmittedInBytes += tupleSizeInBytes;
  }

  public void addFailTuple(HeronTuples.AckTuple.Builder newTuple, long tupleSizeInBytes) {
    if (currentControlTuple == null
        || currentControlTuple.getAcksCount() > 0
        || currentControlTuple.getFailsCount() >= controlTupleSetCapacity) {
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
      bldr.setSrcTaskId(helper.getMyTaskId());
      bldr.setData(currentDataTuple);

      pushTupleToQueue(bldr, outQueue);

      currentDataTuple = null;
    }
    if (currentControlTuple != null) {
      HeronTuples.HeronTupleSet.Builder bldr = HeronTuples.HeronTupleSet.newBuilder();
      bldr.setSrcTaskId(helper.getMyTaskId());
      bldr.setControl(currentControlTuple);

      pushTupleToQueue(bldr, outQueue);

      currentControlTuple = null;
    }
  }

  private void pushTupleToQueue(HeronTuples.HeronTupleSet.Builder bldr,
                                Communicator<Message> out) {
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

  public void updatePhysicalPlanHelper(PhysicalPlanHelper physicalPlanHelper) {
    this.helper = physicalPlanHelper;
  }
}
