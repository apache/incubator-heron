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

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.common.utils.tuple.TupleImpl;
import com.twitter.heron.instance.IInstance;
import com.twitter.heron.proto.ckptmgr.CheckpointManager;
import com.twitter.heron.proto.system.HeronTuples;

public class BoltInstance
    extends com.twitter.heron.instance.bolt.BoltInstance implements IInstance {

  private final Duration instanceExecuteBatchTime;
  private final ByteAmount instanceExecuteBatchSize;

  public BoltInstance(PhysicalPlanHelper helper,
                      Communicator<Message> streamInQueue,
                      Communicator<Message> streamOutQueue,
                      SlaveLooper looper) {
    super(helper, streamInQueue, streamOutQueue, looper);
    SystemConfig systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    this.instanceExecuteBatchTime = systemConfig.getInstanceExecuteBatchTime();
    this.instanceExecuteBatchSize = systemConfig.getInstanceExecuteBatchSize();

  }

  private void handleDataTuple(HeronTuples.HeronDataTuple dataTuple,
                               TopologyAPI.StreamId stream,
                               int srcTaskId) {
    long startTime = System.nanoTime();

    List<Object> values = new ArrayList<>();
    for (ByteString b : dataTuple.getValuesList()) {
      values.add(serializer.deserialize(b.toByteArray()));
    }

    // Decode the tuple
    TupleImpl t = new TupleImpl(helper.getTopologyContext(), stream, dataTuple.getKey(),
        dataTuple.getRootsList(), values, srcTaskId);

    long deserializedTime = System.nanoTime();

    // Delegate to the use defined bolt
    bolt.execute(t);

    Duration executeLatency = Duration.ofNanos(System.nanoTime()).minusNanos(deserializedTime);

    // Invoke user-defined execute task hook
    helper.getTopologyContext().invokeHookBoltExecute(t, executeLatency);

    boltMetrics.deserializeDataTuple(stream.getId(), stream.getComponentName(),
        deserializedTime - startTime);

    // Update metrics
    boltMetrics.executeTuple(stream.getId(), stream.getComponentName(), executeLatency.toNanos());
  }

  @Override
  public void readTuplesAndExecute(Communicator<Message> inQueue) {
    long startOfCycle = System.nanoTime();

    long totalDataEmittedInBytesBeforeCycle = collector.getTotalDataEmittedInBytes();

    // Read data from in Queues
    while (!inQueue.isEmpty()) {
      Message msg = inQueue.poll();

      if (msg instanceof CheckpointManager.InitiateStatefulCheckpoint) {
        persistState(((CheckpointManager.InitiateStatefulCheckpoint) msg).getCheckpointId());
      }

      if (msg instanceof HeronTuples.HeronTupleSet) {
        HeronTuples.HeronTupleSet tuples = (HeronTuples.HeronTupleSet) msg;

        // Handle the tuples
        if (tuples.hasControl()) {
          throw new RuntimeException("Bolt cannot get acks/fails from other components");
        }
        TopologyAPI.StreamId stream = tuples.getData().getStream();

        for (HeronTuples.HeronDataTuple dataTuple : tuples.getData().getTuplesList()) {
          handleDataTuple(dataTuple, stream, tuples.getSrcTaskId());
        }

        // To avoid spending too much time
        if (System.nanoTime() - startOfCycle - instanceExecuteBatchTime.toNanos() > 0) {
          break;
        }

        // To avoid emitting too much data
        if (collector.getTotalDataEmittedInBytes() - totalDataEmittedInBytesBeforeCycle
            > instanceExecuteBatchSize.asBytes()) {
          break;
        }
      }
    }
  }
}
