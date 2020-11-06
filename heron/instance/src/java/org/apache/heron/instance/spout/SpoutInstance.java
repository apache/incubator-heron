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

import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.apache.heron.api.Config;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.metric.GlobalMetrics;
import org.apache.heron.api.serializer.IPluggableSerializer;
import org.apache.heron.api.spout.ISpout;
import org.apache.heron.api.spout.SpoutOutputCollector;
import org.apache.heron.api.state.State;
import org.apache.heron.api.topology.IStatefulComponent;
import org.apache.heron.api.topology.ITwoPhaseStatefulComponent;
import org.apache.heron.api.topology.IUpdatable;
import org.apache.heron.api.utils.Utils;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.ExecutorLooper;
import org.apache.heron.common.basics.FileUtils;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.basics.TypeUtils;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.utils.metrics.FullSpoutMetrics;
import org.apache.heron.common.utils.metrics.ISpoutMetrics;
import org.apache.heron.common.utils.misc.PhysicalPlanHelper;
import org.apache.heron.common.utils.misc.SerializeDeSerializeHelper;
import org.apache.heron.common.utils.topology.TopologyContextImpl;
import org.apache.heron.instance.IInstance;
import org.apache.heron.instance.util.InstanceUtils;
import org.apache.heron.proto.ckptmgr.CheckpointManager;
import org.apache.heron.proto.system.HeronTuples;

public class SpoutInstance implements IInstance {
  private static final Logger LOG = Logger.getLogger(SpoutInstance.class.getName());

  protected final ISpout spout;
  protected final SpoutOutputCollectorImpl collector;
  protected final ISpoutMetrics spoutMetrics;
  // The spout will read Control tuples from streamInQueue
  private final Communicator<Message> streamInQueue;

  private final boolean ackEnabled;
  private final boolean enableMessageTimeouts;

  private final boolean isTopologyStateful;
  private final boolean spillState;
  private final String spillStateLocation;

  // default to false, can only be toggled to true if spout implements ITwoPhaseStatefulComponent
  private boolean waitingForCheckpointSaved;

  private State<Serializable, Serializable> instanceState;

  private final ExecutorLooper looper;

  private final SystemConfig systemConfig;

  // The reference to topology's config
  private final Map<String, Object> config;

  private PhysicalPlanHelper helper;

  /**
   * Construct a SpoutInstance basing on given arguments
   */
  public SpoutInstance(PhysicalPlanHelper helper,
                       Communicator<Message> streamInQueue,
                       Communicator<Message> streamOutQueue,
                       ExecutorLooper looper) {
    this.helper = helper;
    this.looper = looper;
    this.streamInQueue = streamInQueue;
    this.spoutMetrics = new FullSpoutMetrics();
    this.spoutMetrics.initMultiCountMetrics(helper);
    this.config = helper.getTopologyContext().getTopologyConfig();
    this.systemConfig = (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(
        SystemConfig.HERON_SYSTEM_CONFIG);
    this.enableMessageTimeouts =
        Boolean.parseBoolean((String) config.get(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS));

    this.isTopologyStateful = String.valueOf(Config.TopologyReliabilityMode.EFFECTIVELY_ONCE)
        .equals(config.get(Config.TOPOLOGY_RELIABILITY_MODE));

    this.spillState =
        Boolean.parseBoolean((String) config.get(Config.TOPOLOGY_STATEFUL_SPILL_STATE));

    this.spillStateLocation = String.format("%s/%s/",
        String.valueOf(config.get(Config.TOPOLOGY_STATEFUL_SPILL_STATE_LOCATION)),
        helper.getMyInstanceId());

    this.waitingForCheckpointSaved = false;

    LOG.info("Is this topology stateful: " + isTopologyStateful);

    if (helper.getMySpout() == null) {
      throw new RuntimeException("HeronSpoutInstance has no spout in physical plan");
    }

    // Get the spout. Notice, in fact, we will always use the deserialization way to get bolt.
    if (helper.getMySpout().getComp().hasSerializedObject()) {
      this.spout = (ISpout) Utils.deserialize(
          helper.getMySpout().getComp().getSerializedObject().toByteArray());
    } else if (helper.getMySpout().getComp().hasClassName()) {
      String spoutClassName = helper.getMySpout().getComp().getClassName();
      try {
        spout = (ISpout) Class.forName(spoutClassName).newInstance();
      } catch (ClassNotFoundException ex) {
        throw new RuntimeException(ex + " Spout class must be in class path.");
      } catch (InstantiationException ex) {
        throw new RuntimeException(ex + " Spout class must be concrete.");
      } catch (IllegalAccessException ex) {
        throw new RuntimeException(ex + " Spout class must have a no-arg constructor.");
      }
    } else {
      throw new RuntimeException("Neither java_object nor java_class_name set for spout");
    }

    IPluggableSerializer serializer = SerializeDeSerializeHelper.getSerializer(config);
    collector = new SpoutOutputCollectorImpl(serializer, helper, streamOutQueue, spoutMetrics);
    this.ackEnabled = collector.isAckEnabled();

    LOG.info("Enable Ack: " + this.ackEnabled);
    LOG.info("EnableMessageTimeouts: " + this.enableMessageTimeouts);
  }

  @Override
  public void update(PhysicalPlanHelper physicalPlanHelper) {
    if (spout instanceof IUpdatable) {
      ((IUpdatable) spout).update(physicalPlanHelper.getTopologyContext());
    }
    collector.updatePhysicalPlanHelper(physicalPlanHelper);

    // Re-prepare the CustomStreamGrouping since the downstream tasks can change
    physicalPlanHelper.prepareForCustomStreamGrouping();
    // Reset the helper
    helper = physicalPlanHelper;
  }

  @Override
  public void persistState(String checkpointId) {
    LOG.info("Persisting state for checkpoint: " + checkpointId);

    if (!isTopologyStateful) {
      throw new RuntimeException("Could not save a non-stateful topology's state");
    }

    // need to synchronize with other OutgoingTupleCollection operations
    // so that topology emit, ack, fail are thread safe
    collector.lock.lock();
    try {
      if (spout instanceof IStatefulComponent) {
        ((IStatefulComponent) spout).preSave(checkpointId);
      }

      if (spout instanceof ITwoPhaseStatefulComponent) {
        waitingForCheckpointSaved = true;
      }
      collector.sendOutState(instanceState, checkpointId, spillState, spillStateLocation);
    } finally {
      collector.lock.unlock();
    }

    LOG.info("State persisted for checkpoint: " + checkpointId);
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(State<Serializable, Serializable> state) {
    TopologyContextImpl topologyContext = helper.getTopologyContext();

    // Initialize the GlobalMetrics
    GlobalMetrics.init(topologyContext, systemConfig.getHeronMetricsExportInterval());

    spoutMetrics.registerMetrics(topologyContext);

    // Initialize the instanceState if the topology is stateful and spout is a stateful component
    if (isTopologyStateful && spout instanceof IStatefulComponent) {
      this.instanceState = state;
      ((IStatefulComponent<Serializable, Serializable>) spout).initState(instanceState);

      if (spillState) {
        if (FileUtils.isDirectoryExists(spillStateLocation)) {
          FileUtils.cleanDir(spillStateLocation);
        } else {
          FileUtils.createDirectory(spillStateLocation);
        }
      }
    }

    spout.open(
        topologyContext.getTopologyConfig(), topologyContext, new SpoutOutputCollector(collector));

    // Invoke user-defined prepare task hook
    topologyContext.invokeHookPrepare();

    // Init the CustomStreamGrouping
    helper.prepareForCustomStreamGrouping();
  }

  @Override
  public void start() {
    // Add spout tasks for execution
    addSpoutsTasks();
  }

  @Override
  public void preRestore(String checkpointId) {
    if (spout instanceof ITwoPhaseStatefulComponent) {
      ((ITwoPhaseStatefulComponent) spout).preRestore(checkpointId);
    }
  }

  @Override
  public void onCheckpointSaved(String checkpointId) {
    if (spout instanceof ITwoPhaseStatefulComponent) {
      ((ITwoPhaseStatefulComponent) spout).postSave(checkpointId);
      waitingForCheckpointSaved = false;
    }
  }

  @Override
  public void clean() {
    // Invoke clean up hook before clean() is called
    helper.getTopologyContext().invokeHookCleanup();

    // Delegate to user-defined clean-up method
    spout.close();

    // Clean the resources we own
    streamInQueue.clear();
    collector.clear();
  }

  @Override
  public void shutdown() {
    clean();
    looper.exitLoop();
  }

  @Override
  public void activate() {
    LOG.info("Spout is activated");
    spout.activate();
  }

  @Override
  public void deactivate() {
    LOG.info("Spout is deactivated");
    spout.deactivate();
  }

  // Tasks happen in every time looper is waken up
  private void addSpoutsTasks() {
    // Register spoutTasks
    Runnable spoutTasks = new Runnable() {
      @Override
      public void run() {
        spoutMetrics.updateTaskRunCount();

        // Check if we have any message to process anyway
        readTuplesAndExecute(streamInQueue);

        // Check whether we should produce more tuples
        if (isProduceTuple()) {
          spoutMetrics.updateProduceTupleCount();
          produceTuple();
          // Though we may execute MAX_READ tuples, finally we will packet it as
          // one outgoingPacket and push to out queues
          // Notice: Tuples are not necessary emitted during nextTuple methods. We could emit
          // tuples as long as we invoke collector.emit(...)
          collector.sendOutTuples();
        }

        if (!collector.isOutQueuesAvailable()) {
          spoutMetrics.updateOutQueueFullCount();
        }

        if (ackEnabled) {
          // Update the pending-to-be-acked tuples counts
          spoutMetrics.updatePendingTuplesCount(collector.numInFlight());
        } else {
          doImmediateAcks();
        }

        // If we have more work to do
        if (isContinueWork()) {
          spoutMetrics.updateContinueWorkCount();
          looper.wakeUp();
        }
      }
    };
    looper.addTasksOnWakeup(spoutTasks);

    // Look for the timeout's tuples
    if (enableMessageTimeouts) {
      lookForTimeouts();
    }

    InstanceUtils.prepareTimerEvents(looper, helper);
  }

  /**
   * Check whether we still need to do more work.
   * When the topology state is in RUNNING:
   * 1. If the out Queue is not full and ack is not enabled, we could just wake up next time
   * to produce more tuples and push to the out Queue
   * <p>
   * 2. It the out Queue is not full but the ack is enabled, we also need to make sure the
   * tuples waiting smaller than msp
   * <p>
   * 3. If there are more to read, we will wake up itself next time when it doWait()
   *
   * @return true Wake up itself directly in next looper.doWait()
   */
  private boolean isContinueWork() {
    long maxSpoutPending = TypeUtils.getLong(config.get(Config.TOPOLOGY_MAX_SPOUT_PENDING));
    return helper.getTopologyState().equals(TopologyAPI.TopologyState.RUNNING)
        &&
        ((!ackEnabled && collector.isOutQueuesAvailable())
            ||
            (ackEnabled
                && collector.isOutQueuesAvailable()
                && collector.numInFlight() < maxSpoutPending)
            ||
            (ackEnabled && !streamInQueue.isEmpty()));
  }

  /**
   * Check whether we could produce tuples, i.e. invoke spout.nextTuple()
   * It is allowed in:
   * 1. Outgoing Stream queue is available
   * 2. Topology State is RUNNING
   * 3. If the Spout implements ITwoPhaseStatefulComponent, not waiting for checkpoint saved message
   *
   * @return true to allow produceTuple() to be invoked
   */
  private boolean isProduceTuple() {
    return collector.isOutQueuesAvailable()
        && helper.getTopologyState().equals(TopologyAPI.TopologyState.RUNNING)
        && !waitingForCheckpointSaved;
  }

  protected void produceTuple() {
    int maxSpoutPending = TypeUtils.getInteger(config.get(Config.TOPOLOGY_MAX_SPOUT_PENDING));

    long totalTuplesEmitted = collector.getTotalTuplesEmitted();
    long totalBytesEmitted = collector.getTotalBytesEmitted();

    Duration instanceEmitBatchTime = systemConfig.getInstanceEmitBatchTime();
    ByteAmount instanceEmitBatchSize = systemConfig.getInstanceEmitBatchSize();

    long startOfCycle = System.nanoTime();

    // We would reuse the System.nanoTime()
    long currentTime = startOfCycle;

    while (!ackEnabled || (maxSpoutPending > collector.numInFlight())) {
      // Delegate to the use defined spout
      spout.nextTuple();

      // Swap
      long startTime = currentTime;
      currentTime = System.nanoTime();

      long latency = currentTime - startTime;
      spoutMetrics.nextTuple(latency);

      long newTotalTuplesEmitted = collector.getTotalTuplesEmitted();
      long newTotalBytesEmitted = collector.getTotalBytesEmitted();
      if (newTotalTuplesEmitted == totalTuplesEmitted) {
        // No tuples to emit....
        break;
      }

      totalTuplesEmitted = newTotalTuplesEmitted;

      // To avoid spending too much time
      if (currentTime - startOfCycle - instanceEmitBatchTime.toNanos() > 0) {
        break;
      }
      if (!ByteAmount.fromBytes(newTotalBytesEmitted - totalBytesEmitted)
           .lessThan(instanceEmitBatchSize)) {
        break;
      }
    }
  }

  private void handleAckTuple(HeronTuples.AckTuple ackTuple, boolean isSuccess) {
    for (HeronTuples.RootId rt : ackTuple.getRootsList()) {
      if (rt.getTaskid() != helper.getMyTaskId()) {
        throw new RuntimeException(String.format("Receiving tuple for task %d in task %d",
            rt.getTaskid(), helper.getMyTaskId()));
      } else {
        long rootId = rt.getKey();
        RootTupleInfo rootTupleInfo = collector.retireInFlight(rootId);

        // This tuple has been removed due to time-out
        if (rootTupleInfo == null) {
          return;
        }
        Object messageId = rootTupleInfo.getMessageId();
        if (messageId != null) {
          Duration latency = Duration.ofNanos(System.nanoTime())
              .minusNanos(rootTupleInfo.getInsertionTime());
          if (isSuccess) {
            invokeAck(messageId, rootTupleInfo.getStreamId(), latency);
          } else {
            invokeFail(messageId, rootTupleInfo.getStreamId(), latency);
          }
        }
      }
    }
  }

  private void lookForTimeouts() {
    Duration timeout = TypeUtils.getDuration(
        config.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS), ChronoUnit.SECONDS);
    int nBucket = systemConfig.getInstanceAcknowledgementNbuckets();
    List<RootTupleInfo> expiredObjects = collector.retireExpired(timeout);
    for (RootTupleInfo rootTupleInfo : expiredObjects) {
      spoutMetrics.timeoutTuple(rootTupleInfo.getStreamId());
      invokeFail(rootTupleInfo.getMessageId(), rootTupleInfo.getStreamId(), timeout);
    }

    Runnable lookForTimeoutsTask = new Runnable() {
      @Override
      public void run() {
        lookForTimeouts();
      }
    };
    looper.registerTimerEvent(timeout.dividedBy(nBucket), lookForTimeoutsTask);
  }

  @Override
  public void readTuplesAndExecute(Communicator<Message> inQueue) {
    // Read data from in Queues
    long startOfCycle = System.nanoTime();
    Duration spoutAckBatchTime = systemConfig.getInstanceAckBatchTime();

    while (!inQueue.isEmpty() && !waitingForCheckpointSaved) {
      Message msg = inQueue.poll();

      if (msg instanceof CheckpointManager.InitiateStatefulCheckpoint) {
        String checkpintId = ((CheckpointManager.InitiateStatefulCheckpoint) msg).getCheckpointId();
        persistState(checkpintId);
      } else if (msg instanceof HeronTuples.HeronTupleSet) {
        HeronTuples.HeronTupleSet tuples = (HeronTuples.HeronTupleSet) msg;
        // For spout, it should read only control tuples(ack&fail)
        if (tuples.hasData()) {
          throw new RuntimeException("Spout cannot get incoming data tuples from other components");
        }

        if (tuples.hasControl()) {
          for (HeronTuples.AckTuple aT : tuples.getControl().getAcksList()) {
            handleAckTuple(aT, true);
          }
          for (HeronTuples.AckTuple aT : tuples.getControl().getFailsList()) {
            handleAckTuple(aT, false);
          }
        }

        // To avoid spending too much time
        if (System.nanoTime() - startOfCycle - spoutAckBatchTime.toNanos() > 0) {
          break;
        }
      } else {
        // Spout instance should not receive any other messages except the above ones
        throw new RuntimeException("Invalid data sent to spout instance");
      }
    }
  }

  private void doImmediateAcks() {
    // In this iteration, we will only look at the immediateAcks size
    // Otherwise, it could be that eveytime we do an ack, the spout is
    // doing generating another tuple leading to an infinite loop
    int s = collector.getImmediateAcks().size();
    for (int i = 0; i < s; ++i) {
      RootTupleInfo tupleInfo = collector.getImmediateAcks().poll();
      invokeAck(tupleInfo.getMessageId(), tupleInfo.getStreamId(), Duration.ZERO);
    }
  }

  private void invokeAck(Object messageId, String streamId, Duration completeLatency) {
    // delegate to user-defined methods
    spout.ack(messageId);

    // Invoke user-defined task hooks
    helper.getTopologyContext().invokeHookSpoutAck(messageId, completeLatency);

    // Update metrics
    spoutMetrics.ackedTuple(streamId, completeLatency.toNanos());
  }

  private void invokeFail(Object messageId, String streamId, Duration failLatency) {
    // delegate to user-defined methods
    spout.fail(messageId);

    // Invoke user-defined task hooks
    helper.getTopologyContext().invokeHookSpoutFail(messageId, failLatency);

    // Update metrics
    spoutMetrics.failedTuple(streamId, failLatency.toNanos());
  }
}
