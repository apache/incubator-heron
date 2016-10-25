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

package com.twitter.heron.instance.spout;

import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.api.metric.GlobalMetrics;
import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.api.spout.ISpout;
import com.twitter.heron.api.spout.SpoutOutputCollector;
import com.twitter.heron.api.topology.IUpdatable;
import com.twitter.heron.api.utils.Utils;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.Constants;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.basics.TypeUtils;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.utils.metrics.SpoutMetrics;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.common.utils.misc.SerializeDeSerializeHelper;
import com.twitter.heron.common.utils.topology.TopologyContextImpl;
import com.twitter.heron.instance.IInstance;
import com.twitter.heron.proto.system.HeronTuples;

public class SpoutInstance implements IInstance {
  private static final Logger LOG = Logger.getLogger(SpoutInstance.class.getName());

  protected final ISpout spout;
  protected final SpoutOutputCollectorImpl collector;
  protected final SpoutMetrics spoutMetrics;
  // The spout will read Control tuples from streamInQueue
  private final Communicator<HeronTuples.HeronTupleSet> streamInQueue;

  private final boolean ackEnabled;
  private final boolean enableMessageTimeouts;

  private final SlaveLooper looper;

  private final SystemConfig systemConfig;

  // The reference to topology's config
  private final Map<String, Object> config;

  private PhysicalPlanHelper helper;

  private TopologyAPI.TopologyState topologyState;

  /**
   * Construct a SpoutInstance basing on given arguments
   */
  public SpoutInstance(PhysicalPlanHelper helper,
                       Communicator<HeronTuples.HeronTupleSet> streamInQueue,
                       Communicator<HeronTuples.HeronTupleSet> streamOutQueue,
                       SlaveLooper looper) {
    this.helper = helper;
    this.looper = looper;
    this.streamInQueue = streamInQueue;
    this.spoutMetrics = new SpoutMetrics();
    this.spoutMetrics.initMultiCountMetrics(helper);
    this.config = helper.getTopologyContext().getTopologyConfig();
    this.systemConfig = (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(
        SystemConfig.HERON_SYSTEM_CONFIG);
    this.ackEnabled = Boolean.parseBoolean((String) config.get(Config.TOPOLOGY_ENABLE_ACKING));
    this.enableMessageTimeouts =
        Boolean.parseBoolean((String) config.get(Config.TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS));

    LOG.info("Enable Ack: " + this.ackEnabled);
    LOG.info("EnableMessageTimeouts: " + this.enableMessageTimeouts);

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
  }

  @Override
  public void update(PhysicalPlanHelper physicalPlanHelper) {
    if (spout instanceof IUpdatable) {
      ((IUpdatable) spout).update(physicalPlanHelper.getTopologyContext());
    }
    collector.updatePhysicalPlanHelper(physicalPlanHelper);
  }

  @Override
  public void start() {
    TopologyContextImpl topologyContext = helper.getTopologyContext();

    // Initialize the GlobalMetrics
    GlobalMetrics.init(topologyContext, systemConfig.getHeronMetricsExportIntervalSec());

    spoutMetrics.registerMetrics(topologyContext);

    spout.open(
        topologyContext.getTopologyConfig(), topologyContext, new SpoutOutputCollector(collector));

    // Invoke user-defined prepare task hook
    topologyContext.invokeHookPrepare();

    // Init the CustomStreamGrouping
    helper.prepareForCustomStreamGrouping();

    // Tasks happen in every time looper is waken up
    addSpoutsTasks();

    topologyState = TopologyAPI.TopologyState.RUNNING;
  }

  @Override
  public void stop() {
    // Invoke clean up hook before clean() is called
    helper.getTopologyContext().invokeHookCleanup();

    // Delegate to user-defined clean-up method
    spout.close();

    // Clean the resources we own
    looper.exitLoop();
    streamInQueue.clear();
    collector.clear();
  }

  @Override
  public void activate() {
    LOG.info("Spout is activated");
    spout.activate();
    topologyState = TopologyAPI.TopologyState.RUNNING;
  }

  @Override
  public void deactivate() {
    LOG.info("Spout is deactivated");
    spout.deactivate();
    topologyState = TopologyAPI.TopologyState.PAUSED;
  }

  // Tasks happen in every time looper is waken up
  private void addSpoutsTasks() {
    // Register spoutTasks
    Runnable spoutTasks = new Runnable() {
      @Override
      public void run() {
        // Check whether we should produce more tuples
        if (isProduceTuple()) {
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
          readTuplesAndExecute(streamInQueue);

          // Update the pending-to-be-acked tuples counts
          spoutMetrics.updatePendingTuplesCount(collector.numInFlight());
        } else {
          doImmediateAcks();
        }

        // If we have more work to do
        if (isContinueWork()) {
          looper.wakeUp();
        }
      }
    };
    looper.addTasksOnWakeup(spoutTasks);

    // Look for the timeout's tuples
    if (enableMessageTimeouts) {
      lookForTimeouts();
    }
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
    return topologyState.equals(TopologyAPI.TopologyState.RUNNING)
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
   *
   * @return true to allow produceTuple() to be invoked
   */
  private boolean isProduceTuple() {
    return collector.isOutQueuesAvailable()
        && topologyState.equals(TopologyAPI.TopologyState.RUNNING);
  }

  protected void produceTuple() {
    int maxSpoutPending = TypeUtils.getInteger(config.get(Config.TOPOLOGY_MAX_SPOUT_PENDING));

    long totalTuplesEmitted = collector.getTotalTuplesEmitted();

    long instanceEmitBatchTime =
        systemConfig.getInstanceEmitBatchTimeMs() * Constants.MILLISECONDS_TO_NANOSECONDS;

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
      if (newTotalTuplesEmitted == totalTuplesEmitted) {
        // No tuples to emit....
        break;
      }

      totalTuplesEmitted = newTotalTuplesEmitted;

      // To avoid spending too much time
      if (currentTime - startOfCycle - instanceEmitBatchTime > 0) {
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
          long latency = System.nanoTime() - rootTupleInfo.getInsertionTime();
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
    long timeoutInSeconds = TypeUtils.getLong(config.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS));
    long timeoutInNs = timeoutInSeconds * Constants.SECONDS_TO_NANOSECONDS;
    int nBucket = systemConfig.getInstanceAcknowledgementNbuckets();
    List<RootTupleInfo> expiredObjects = collector.retireExpired(timeoutInNs);
    for (RootTupleInfo rootTupleInfo : expiredObjects) {
      spoutMetrics.timeoutTuple(rootTupleInfo.getStreamId());
      invokeFail(rootTupleInfo.getMessageId(), rootTupleInfo.getStreamId(), timeoutInNs);
    }

    Runnable lookForTimeoutsTask = new Runnable() {
      @Override
      public void run() {
        lookForTimeouts();
      }
    };
    looper.registerTimerEventInNanoSeconds(timeoutInNs / nBucket, lookForTimeoutsTask);
  }

  @Override
  public void readTuplesAndExecute(Communicator<HeronTuples.HeronTupleSet> inQueue) {
    // Read data from in Queues
    long startOfCycle = System.nanoTime();
    long spoutAckBatchTime = systemConfig.getInstanceAckBatchTimeMs()
        * Constants.MILLISECONDS_TO_NANOSECONDS;

    while (!inQueue.isEmpty()) {
      HeronTuples.HeronTupleSet tuples = inQueue.poll();
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
      if (System.nanoTime() - startOfCycle - spoutAckBatchTime > 0) {
        break;
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
      invokeAck(tupleInfo.getMessageId(), tupleInfo.getStreamId(), 0L);
    }
  }

  private void invokeAck(Object messageId, String streamId, Long completeLatencyNs) {
    // delegate to user-defined methods
    spout.ack(messageId);

    // Invoke user-defined task hooks
    helper.getTopologyContext().invokeHookSpoutAck(messageId, completeLatencyNs);

    // Update metrics
    spoutMetrics.ackedTuple(streamId, completeLatencyNs);
  }

  private void invokeFail(Object messageId, String streamId, Long failLatencyNs) {
    // delegate to user-defined methods
    spout.fail(messageId);

    // Invoke user-defined task hooks
    helper.getTopologyContext().invokeHookSpoutFail(messageId, failLatencyNs);

    // Update metrics
    spoutMetrics.failedTuple(streamId, failLatencyNs);
  }
}
