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

package com.twitter.heron.simulator.executors;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.basics.WakeableLooper;
import com.twitter.heron.proto.stmgr.StreamManager;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.simulator.utils.PhysicalPlanUtil;
import com.twitter.heron.simulator.utils.StreamConsumers;
import com.twitter.heron.simulator.utils.TupleCache;
import com.twitter.heron.simulator.utils.XORManager;

public class StreamExecutor implements Runnable {
  public static final int NUM_BUCKETS = 3;

  private static final Logger LOG = Logger.getLogger(InstanceExecutor.class.getName());

  // TaskId -> InstanceExecutor
  private final Map<Integer, InstanceExecutor> taskIdToInstanceExecutor;

  private final Map<TopologyAPI.StreamId, StreamConsumers> streamIdStreamConsumersMap;

  private final Set<String> spoutSets;

  private final XORManager xorManager;

  private final TupleCache tupleCache;

  private final WakeableLooper looper;

  public StreamExecutor(PhysicalPlans.PhysicalPlan pPlan) {
    this.taskIdToInstanceExecutor = new HashMap<>();
    this.looper = createWakeableLooper();

    this.spoutSets = createSpoutsSet(pPlan);

    Map<String, List<Integer>> componentToTaskIds =
        PhysicalPlanUtil.getComponentToTaskIds(pPlan);

    this.streamIdStreamConsumersMap =
        StreamConsumers.populateStreamConsumers(pPlan.getTopology(), componentToTaskIds);

    this.xorManager =
        XORManager.populateXORManager(looper, pPlan.getTopology(), NUM_BUCKETS, componentToTaskIds);

    this.tupleCache = new TupleCache();
  }

  public void addInstanceExecutor(InstanceExecutor instanceExecutor) {
    // Set the InstanceExecutor's streamOutQueue's consumer
    instanceExecutor.getStreamOutQueue().setConsumer(looper);

    // Set the InstanceExecutor's streamInQueue's producer
    instanceExecutor.getStreamInQueue().setProducer(looper);

    taskIdToInstanceExecutor.put(instanceExecutor.getTaskId(), instanceExecutor);
  }

  @Override
  public void run() {
    Thread.currentThread().setName("Simulator_Stream_Executor");

    LOG.info("Stream_Executor starts");

    addStreamExecutorTasks();
    looper.loop();
  }

  public void stop() {
    looper.exitLoop();
  }

  protected void addStreamExecutorTasks() {
    // 1. Insert data tuples to cache

    // 2. If acking is enabled, do the acking management

    // 3. If needed, send back acking tuples

    // 4. Eventually route the tuples to correct places

    Runnable streamExecutorsTasks = new Runnable() {
      @Override
      public void run() {
        // When Wakelooper is running, if it is notified by other WakeLooper,
        // It would wake up immediately in next time's wait().
        // So it would not enter dead-lock condition
        if (tupleCache.isEmpty()) {
          // We would read stream from instance executor only when the cache is drained
          // We do this to trigger back-pressure to avoid too many live objects in memory
          handleInstanceExecutor();
        }

        drainCache();
      }
    };

    looper.addTasksOnWakeup(streamExecutorsTasks);
  }

  /**
   * Handle the execution of the instance
   */
  public void handleInstanceExecutor() {
    for (InstanceExecutor executor : taskIdToInstanceExecutor.values()) {
      boolean isLocalSpout = spoutSets.contains(executor.getComponentName());
      int taskId = executor.getTaskId();

      int items = executor.getStreamOutQueue().size();
      for (int i = 0; i < items; i++) {
        HeronTuples.HeronTupleSet tupleSet = executor.getStreamOutQueue().poll();
        if (tupleSet == null) {
          // No stream from this queue
          break;
        }

        if (tupleSet.hasData()) {
          HeronTuples.HeronDataTupleSet d = tupleSet.getData();
          TopologyAPI.StreamId streamId = d.getStream();
          StreamConsumers consumers = streamIdStreamConsumersMap.get(streamId);
          if (consumers != null) {
            for (HeronTuples.HeronDataTuple tuple : d.getTuplesList()) {
              List<Integer> outTasks = consumers.getListToSend(tuple);

              outTasks.addAll(tuple.getDestTaskIdsList());

              if (outTasks.isEmpty()) {
                LOG.severe("Nobody to sent the tuple to");
              }

              copyDataOutBound(taskId, isLocalSpout, streamId, tuple, outTasks);
            }
          } else {
            LOG.severe("Nobody consumes stream: " + streamId);
          }
        }

        if (tupleSet.hasControl()) {
          HeronTuples.HeronControlTupleSet c = tupleSet.getControl();
          for (HeronTuples.AckTuple ack : c.getAcksList()) {
            copyControlOutBound(ack, true);
          }

          for (HeronTuples.AckTuple fail : c.getFailsList()) {
            copyControlOutBound(fail, false);
          }
        }
      }
    }
  }

  // Check whether target destination task has free room to receive more tuples
  protected boolean isSendTuplesToInstance(List<Integer> taskIds) {
    for (Integer taskId : taskIds) {
      if (taskIdToInstanceExecutor.get(taskId).getStreamInQueue().remainingCapacity() <= 0) {
        return false;
      }
    }

    return true;
  }

  // Process HeronDataTuple and insert it into cache
  protected void copyDataOutBound(int sourceTaskId,
                                  boolean isLocalSpout,
                                  TopologyAPI.StreamId streamId,
                                  HeronTuples.HeronDataTuple tuple,
                                  List<Integer> outTasks) {
    boolean firstIteration = true;
    boolean isAnchored = tuple.getRootsCount() > 0;

    for (Integer outTask : outTasks) {
      long tupleKey = tupleCache.addDataTuple(outTask, streamId, tuple, isAnchored);
      if (isAnchored) {
        // Anchored tuple

        if (isLocalSpout) {
          // This is from a local spout. We need to maintain xors
          if (firstIteration) {
            xorManager.create(sourceTaskId, tuple.getRoots(0).getKey(), tupleKey);
          } else {
            xorManager.anchor(sourceTaskId, tuple.getRoots(0).getKey(), tupleKey);
          }
        } else {
          // Anchored emits from local bolt
          for (HeronTuples.RootId rootId : tuple.getRootsList()) {
            HeronTuples.AckTuple t =
                HeronTuples.AckTuple.newBuilder().
                    addRoots(rootId).
                    setAckedtuple(tupleKey).
                    build();

            tupleCache.addEmitTuple(rootId.getTaskid(), t);
          }
        }
      }

      firstIteration = false;
    }
  }

  // Process HeronAckTuple and insert it into cache
  protected void copyControlOutBound(HeronTuples.AckTuple control,
                                     boolean isSuccess) {
    for (HeronTuples.RootId rootId : control.getRootsList()) {
      HeronTuples.AckTuple t =
          HeronTuples.AckTuple.newBuilder().
              addRoots(rootId).
              setAckedtuple(control.getAckedtuple()).
              build();

      if (isSuccess) {
        tupleCache.addAckTuple(rootId.getTaskid(), t);
      } else {
        tupleCache.addFailTuple(rootId.getTaskid(), t);
      }
    }
  }

  // Do the XOR control and send the ack tuples to instance if necessary
  protected void processAcksAndFails(int taskId,
                                     HeronTuples.HeronControlTupleSet controlTupleSet) {
    StreamManager.TupleMessage.Builder out = StreamManager.TupleMessage.newBuilder();

    // First go over emits. This makes sure that new emits makes
    // a tuples stay alive before we process its acks
    for (HeronTuples.AckTuple emitTuple : controlTupleSet.getEmitsList()) {
      for (HeronTuples.RootId rootId : emitTuple.getRootsList()) {
        xorManager.anchor(taskId, rootId.getKey(), emitTuple.getAckedtuple());
      }
    }

    // Then go over acks
    for (HeronTuples.AckTuple ackTuple : controlTupleSet.getAcksList()) {
      for (HeronTuples.RootId rootId : ackTuple.getRootsList()) {
        if (xorManager.anchor(taskId, rootId.getKey(), ackTuple.getAckedtuple())) {
          // This tuple tree is all over

          HeronTuples.AckTuple.Builder a = out.getSetBuilder().getControlBuilder().addAcksBuilder();
          HeronTuples.RootId.Builder r = a.addRootsBuilder();

          r.setKey(rootId.getKey());
          r.setTaskid(taskId);

          a.setAckedtuple(0); //  This is ignored

          xorManager.remove(taskId, rootId.getKey());
        }
      }
    }

    // Now go over the fails
    for (HeronTuples.AckTuple failTuple : controlTupleSet.getFailsList()) {
      for (HeronTuples.RootId rootId : failTuple.getRootsList()) {
        if (xorManager.remove(taskId, rootId.getKey())) {
          // This tuple tree is failed

          HeronTuples.AckTuple.Builder f =
              out.getSetBuilder().getControlBuilder().addFailsBuilder();
          HeronTuples.RootId.Builder r = f.addRootsBuilder();

          r.setKey(rootId.getKey());
          r.setTaskid(taskId);
          f.setAckedtuple(0); //  This is ignored
        }
      }
    }

    // Check if we need to send ack tuples to spout task
    if (out.hasSet()) {
      sendMessageToInstance(taskId, out.build());
    }
  }

  // Drain the TupleCache if there are room in destination tasks
  protected void drainCache() {
    // Route the tuples to correct places
    Map<Integer, List<HeronTuples.HeronTupleSet>> cache = tupleCache.getCache();

    if (!isSendTuplesToInstance(new LinkedList<Integer>(cache.keySet()))) {
      // Check whether we could send tuples
      return;
    }

    for (Map.Entry<Integer, List<HeronTuples.HeronTupleSet>> entry : cache.entrySet()) {
      int taskId = entry.getKey();
      for (HeronTuples.HeronTupleSet message : entry.getValue()) {
        sendInBound(taskId, message);
      }
    }

    // Reset the tupleCache
    tupleCache.clear();
  }

  // Send Stream to instance
  protected void sendInBound(int taskId, HeronTuples.HeronTupleSet message) {
    if (message.hasData()) {
      StreamManager.TupleMessage.Builder out = StreamManager.TupleMessage.newBuilder();
      out.getSetBuilder().setData(message.getData());

      sendMessageToInstance(taskId, out.build());
    }

    if (message.hasControl()) {
      processAcksAndFails(taskId, message.getControl());
    }
  }

  // Send one message to target task
  protected void sendMessageToInstance(int taskId, StreamManager.TupleMessage message) {
    taskIdToInstanceExecutor.get(taskId).getStreamInQueue().offer(message.getSet());
  }

  protected WakeableLooper createWakeableLooper() {
    return new SlaveLooper();
  }

  protected Set<String> createSpoutsSet(PhysicalPlans.PhysicalPlan physicalPlan) {
    Set<String> spoutsSet = new HashSet<>();
    for (TopologyAPI.Spout spout : physicalPlan.getTopology().getSpoutsList()) {
      spoutsSet.add(spout.getComp().getName());
    }

    return spoutsSet;
  }
}
