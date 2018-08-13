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

package org.apache.heron.instance;

import java.io.Serializable;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.serializer.IPluggableSerializer;
import org.apache.heron.api.state.State;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.FileUtils;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.utils.metrics.ComponentMetrics;
import org.apache.heron.common.utils.misc.PhysicalPlanHelper;
import org.apache.heron.common.utils.misc.SerializeDeSerializeHelper;
import org.apache.heron.proto.ckptmgr.CheckpointManager;
import org.apache.heron.proto.system.HeronTuples;

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
  private AtomicLong totalDataEmittedInBytes = new AtomicLong();

  // Current size in bytes for data types to pack into the HeronTupleSet
  private long currentDataTupleSizeInBytes;

  private final ReentrantLock lock;

  protected final ComponentMetrics metrics;

  public OutgoingTupleCollection(
      PhysicalPlanHelper helper,
      Communicator<Message> outQueue,
      ReentrantLock lock,
      ComponentMetrics metrics) {
    this.outQueue = outQueue;
    this.helper = helper;
    this.metrics = metrics;
    SystemConfig systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    this.serializer =
        SerializeDeSerializeHelper.getSerializer(helper.getTopologyContext().getTopologyConfig());

    // Initialize the values in constructor
    this.totalDataEmittedInBytes.set(0);
    this.currentDataTupleSizeInBytes = 0;

    // Read the config values
    this.dataTupleSetCapacity = systemConfig.getInstanceSetDataTupleCapacity();
    this.maxDataTupleSize = systemConfig.getInstanceSetDataTupleSize();
    this.controlTupleSetCapacity = systemConfig.getInstanceSetControlTupleCapacity();
    this.lock = lock;
  }

  public void sendOutTuples() {
    lock.lock();
    try {
      flushRemaining();
    } finally {
      lock.unlock();
    }
  }

  /**
   * Send out the instance's state with corresponding checkpointId. If spillState is True,
   * the actual state is spill to disk and only the state location is sent out.
   * @param state instance's state
   * @param checkpointId the checkpointId
   * @param spillState spill the state to local disk if true
   * @param location the location where state is spilled
   */
  public void sendOutState(State<Serializable, Serializable> state,
                           String checkpointId,
                           boolean spillState,
                           String location) {
    lock.lock();
    try {
      // flush all the current data before sending the state
      flushRemaining();

      // serialize the state
      byte[] serializedState = serializer.serialize(state);

      CheckpointManager.InstanceStateCheckpoint.Builder instanceStateBuilder =
          CheckpointManager.InstanceStateCheckpoint.newBuilder();
      instanceStateBuilder.setCheckpointId(checkpointId);

      if (spillState) {
        FileUtils.cleanDir(location);

        String stateLocation = location + checkpointId + "-" + UUID.randomUUID();
        if (!FileUtils.writeToFile(stateLocation, serializedState, true)) {
          throw new RuntimeException("failed to spill state. Bailing out...");
        }
        instanceStateBuilder.setStateLocation(stateLocation);
      } else {
        instanceStateBuilder.setState(ByteString.copyFrom(serializedState));
      }

      CheckpointManager.StoreInstanceStateCheckpoint storeRequest =
          CheckpointManager.StoreInstanceStateCheckpoint.newBuilder()
              .setState(instanceStateBuilder.build())
              .build();

      // Put the checkpoint to out stream queue
      outQueue.offer(storeRequest);
    } finally {
      lock.unlock();
    }
  }

  public void addDataTuple(
      String streamId,
      HeronTuples.HeronDataTuple.Builder newTuple,
      long tupleSizeInBytes) {
    lock.lock();
    try {
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
      totalDataEmittedInBytes.getAndAdd(tupleSizeInBytes);
    } finally {
      lock.unlock();
    }
  }

  public void addAckTuple(
      HeronTuples.AckTuple.Builder newTuple, long tupleSizeInBytes) {
    lock.lock();
    try {
      if (currentControlTuple == null
          || currentControlTuple.getFailsCount() > 0
          || currentControlTuple.getAcksCount() >= controlTupleSetCapacity) {
        initNewControlTuple();
      }
      currentControlTuple.addAcks(newTuple);

      // Add the size of data in bytes ready to send out
      totalDataEmittedInBytes.getAndAdd(tupleSizeInBytes);
    } finally {
      lock.unlock();
    }
  }

  public void addFailTuple(
      HeronTuples.AckTuple.Builder newTuple, long tupleSizeInBytes) {
    lock.lock();
    try {
      if (currentControlTuple == null
          || currentControlTuple.getAcksCount() > 0
          || currentControlTuple.getFailsCount() >= controlTupleSetCapacity) {
        initNewControlTuple();
      }
      currentControlTuple.addFails(newTuple);

      // Add the size of data in bytes ready to send out
      totalDataEmittedInBytes.getAndAdd(tupleSizeInBytes);
    } finally {
      lock.unlock();
    }
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
      metrics.addTupleToQueue(currentDataTuple.getTuplesCount());

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
    return totalDataEmittedInBytes.get();
  }

  // Clean the internal state of OutgoingTupleCollection
  public void clear() {
    lock.lock();
    try {
      currentControlTuple = null;
      currentDataTuple = null;

      outQueue.clear();
    } finally {
      lock.unlock();
    }
  }

  public void updatePhysicalPlanHelper(PhysicalPlanHelper physicalPlanHelper) {
    lock.lock();
    try {
      this.helper = physicalPlanHelper;
    } finally {
      lock.unlock();
    }
  }
}
