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
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.apache.heron.api.Config;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.serializer.IPluggableSerializer;
import org.apache.heron.api.state.HashMapState;
import org.apache.heron.api.state.State;
import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.ExecutorLooper;
import org.apache.heron.common.basics.FileUtils;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.utils.metrics.MetricsCollector;
import org.apache.heron.common.utils.misc.PhysicalPlanHelper;
import org.apache.heron.common.utils.misc.SerializeDeSerializeHelper;
import org.apache.heron.common.utils.misc.ThreadNames;
import org.apache.heron.instance.bolt.BoltInstance;
import org.apache.heron.instance.spout.SpoutInstance;
import org.apache.heron.proto.ckptmgr.CheckpointManager;
import org.apache.heron.proto.system.Common;
import org.apache.heron.proto.system.Metrics;

/**
 * The executor, which in fact is a InstanceFactory, creates a new spout or bolt according to the PhysicalPlan.
 * First, if the instance is null, it will wait for the PhysicalPlan from inQueue and, if it receives one,
 * will instantiate a new instance (spout or bolt) according to the PhysicalPlanHelper in SingletonRegistry.
 * It is a Runnable so it could be executed in a Thread. During run(), it will begin the ExecutorLooper's loop().
 */
public class Executor implements Runnable, AutoCloseable {
  private static final Logger LOG = Logger.getLogger(Executor.class.getName());

  private final ExecutorLooper executorLooper;
  private MetricsCollector metricsCollector;
  // Communicator
  private final Communicator<Message> streamInCommunicator;
  private final Communicator<Message> streamOutCommunicator;
  private final Communicator<InstanceControlMsg> inControlQueue;
  private final Communicator<Metrics.MetricPublisherPublishMessage> metricsOutCommunicator;
  private IPluggableSerializer serializer;
  private IInstance instance;
  private PhysicalPlanHelper helper;
  private SystemConfig systemConfig;

  private boolean isInstanceStarted;

  private State<Serializable, Serializable> instanceState;
  private boolean isStatefulProcessingStarted;

  public Executor(ExecutorLooper executorLooper,
               final Communicator<Message> streamInCommunicator,
               final Communicator<Message> streamOutCommunicator,
               final Communicator<InstanceControlMsg> inControlQueue,
               final Communicator<Metrics.MetricPublisherPublishMessage> metricsOutCommunicator) {
    this.executorLooper = executorLooper;
    this.streamInCommunicator = streamInCommunicator;
    this.streamOutCommunicator = streamOutCommunicator;
    this.inControlQueue = inControlQueue;
    this.metricsOutCommunicator = metricsOutCommunicator;

    isInstanceStarted = false;

    // The instance state will be provided by stream manager with RestoreInstanceStateRequests
    instanceState = null;
    isStatefulProcessingStarted = false;

    this.systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    this.metricsCollector = new MetricsCollector(executorLooper, metricsOutCommunicator);

    handleControlMessage();
  }

  private void handleControlMessage() {
    Runnable handleControlMessageTask = new Runnable() {
      @Override
      public void run() {
        while (!inControlQueue.isEmpty()) {
          InstanceControlMsg instanceControlMsg = inControlQueue.poll();

          // Handle start stateful processing request
          // Pre-condition: This message is received after RestoreInstanceStateRequest
          if (instanceControlMsg.isStartInstanceStatefulProcessing()) {
            handleStartInstanceStatefulProcessing(instanceControlMsg);
          }

          // Handle restore instance state request
          // It can happen in 2 cases:
          // 1. Startup -- there will always be at least one physical plan coming
          // before or after the RestoreInstanceStateRequest
          // 2. Normal running -- there may not be any new physical plan
          if (instanceControlMsg.isRestoreInstanceStateRequest()) {
            handleRestoreInstanceStateRequest(instanceControlMsg);
          }

          // Handle New Physical Plan
          if (instanceControlMsg.isNewPhysicalPlanHelper()) {
            handleNewPhysicalPlan(instanceControlMsg);
          }

          // When a checkpoint becomes "globally consistent"
          if (instanceControlMsg.getStatefulCheckpointSavedMessage() != null) {
            String checkpointId = instanceControlMsg
                .getStatefulCheckpointSavedMessage()
                .getConsistentCheckpoint()
                .getCheckpointId();

            handleGlobalCheckpointConsistent(checkpointId);
          }
        }
      }
    };

    executorLooper.addTasksOnWakeup(handleControlMessageTask);
  }

  private void handleGlobalCheckpointConsistent(String checkpointId) {
    LOG.log(Level.INFO, "checkpoint: {0} has become globally consistent", checkpointId);
    instance.onCheckpointSaved(checkpointId);
  }

  private void resetCurrentAssignment() {
    helper.setTopologyContext(metricsCollector);
    instance = helper.getMySpout() != null
        ? new SpoutInstance(helper, streamInCommunicator, streamOutCommunicator, executorLooper)
        : new BoltInstance(helper, streamInCommunicator, streamOutCommunicator, executorLooper);

    startInstanceIfNeeded();
  }

  private void handleNewAssignment() {
    LOG.log(Level.INFO,
        "Incarnating ourselves as {0} with task id {1}",
        new Object[]{helper.getMyComponent(), helper.getMyTaskId()});

    // Initialize serializer once we got the new physical plan
    this.serializer =
        SerializeDeSerializeHelper.getSerializer(helper.getTopologyContext().getTopologyConfig());

    // During the initiation of instance,
    // we would add a bunch of tasks to executorLooper's tasksOnWakeup
    if (helper.getMySpout() != null) {
      instance =
          new SpoutInstance(helper, streamInCommunicator, streamOutCommunicator, executorLooper);

      streamInCommunicator.init(systemConfig.getInstanceInternalSpoutReadQueueCapacity(),
          systemConfig.getInstanceTuningExpectedSpoutReadQueueSize(),
          systemConfig.getInstanceTuningCurrentSampleWeight());
      streamOutCommunicator.init(systemConfig.getInstanceInternalSpoutWriteQueueCapacity(),
          systemConfig.getInstanceTuningExpectedSpoutWriteQueueSize(),
          systemConfig.getInstanceTuningCurrentSampleWeight());
    } else {
      instance =
          new BoltInstance(helper, streamInCommunicator, streamOutCommunicator, executorLooper);

      streamInCommunicator.init(systemConfig.getInstanceInternalBoltReadQueueCapacity(),
          systemConfig.getInstanceTuningExpectedBoltReadQueueSize(),
          systemConfig.getInstanceTuningCurrentSampleWeight());
      streamOutCommunicator.init(systemConfig.getInstanceInternalBoltWriteQueueCapacity(),
          systemConfig.getInstanceTuningExpectedBoltWriteQueueSize(),
          systemConfig.getInstanceTuningCurrentSampleWeight());
    }

    if (!helper.isTopologyRunning()) {
      LOG.info("Instance is deployed in deactivated state");
    }

    startInstanceIfNeeded();
  }

  @Override
  public void run() {
    Thread.currentThread().setName(ThreadNames.THREAD_EXECUTOR_NAME);

    executorLooper.loop();
  }

  @SuppressWarnings("unchecked")
  private void startInstanceIfNeeded() {
    // To start the instance when:
    //  1. We got the PhysicalPlan
    //  2. The TopologyState == RUNNING
    //  3. - If the topology is stateful and we got the stateful processing start signal
    //     - If the topology is not stateful
    if (helper == null) {
      LOG.info("No physical plan received. Instance is not started");
      return;
    }

    // Setting topology environment properties
    Map<String, Object> topoConf = helper.getTopologyContext().getTopologyConfig();
    if (topoConf.containsKey(Config.TOPOLOGY_ENVIRONMENT)) {
      Map<String, String> envProps =
          (Map<String, String>) topoConf.get(Config.TOPOLOGY_ENVIRONMENT);
      LOG.info("Setting topology environment: " + envProps);
      System.getProperties().putAll(envProps);
    }

    if (helper.isTopologyStateful()) {
      // For stateful topology, `init(state)` will be invoked
      // when a RestoreInstanceStateRequest is received
      if (isStatefulProcessingStarted) {
        instance.init(instanceState);
        // Deactivate the instance if start with deactivated mode
        if (TopologyAPI.TopologyState.PAUSED.equals(helper.getTopologyState())) {
          instance.deactivate();
        }
        instance.start();
        isInstanceStarted = true;
        LOG.info("Instance is started for stateful topology");
      } else {
        LOG.info("Start signal not received. Instance is not started");
      }
    } else {
      // For non-stateful topology, any provided state will be ignored
      // by the instance
      instance.init(null);
      // Deactivate the instance if start with deactivated mode
      if (TopologyAPI.TopologyState.PAUSED.equals(helper.getTopologyState())) {
        instance.deactivate();
      }
      instance.start();
      isInstanceStarted = true;
      LOG.info("Instance is started for non-stateful topology");
    }
  }

  public void close() {
    LOG.info("Closing the Executor Thread");
    this.metricsCollector.forceGatherAllMetrics();
    LOG.info("Shutting down the instance");
    if (instance != null) {
      instance.shutdown();
    }

    // Clean the resources we own
    executorLooper.exitLoop();
    streamInCommunicator.clear();
    // The clean of out stream communicator will be handled by instance itself
  }

  private void handleStartInstanceStatefulProcessing(InstanceControlMsg instanceControlMsg) {
    CheckpointManager.StartInstanceStatefulProcessing startStatefulRequest =
        instanceControlMsg.getStartInstanceStatefulProcessing();
    LOG.info("Starting stateful processing with checkpoint id: "
        + startStatefulRequest.getCheckpointId());
    isStatefulProcessingStarted = true;

    // At this point, the pre-condition is we have already created the actual instance
    // and initialized it properly. Check if we can start the topology
    startInstanceIfNeeded();
  }

  private void cleanAndStopExecutorBeforeRestore(String checkpointId) {
    // Clear all queues
    streamInCommunicator.clear();
    streamOutCommunicator.clear();

    // Flash out existing metrics
    metricsCollector.forceGatherAllMetrics();

    // Stop executor looper consuming data/control_msg
    executorLooper.clearTasksOnWakeup();
    executorLooper.clearTimers();

    if (instance != null) {
      instance.preRestore(checkpointId);
      instance.clean();
    }

    isStatefulProcessingStarted = false;
  }

  private void registerTasksWithExecutor() {
    // Create a new MetricsCollector with the clean executorLooper and register its task
    metricsCollector = new MetricsCollector(executorLooper, metricsOutCommunicator);

    // registering the handling of control msg
    handleControlMessage();

    // instance task will be registered when instance.start() is called
  }

  private void handleRestoreInstanceStateRequest(InstanceControlMsg instanceControlMsg) {
    CheckpointManager.RestoreInstanceStateRequest request =
        instanceControlMsg.getRestoreInstanceStateRequest();

    // ID of the checkpoint we are restoring to
    String checkpointId = request.getState().getCheckpointId();

    // Clean buffers and unregister tasks in executor looper
    if (isInstanceStarted) {
      cleanAndStopExecutorBeforeRestore(checkpointId);
    }

    // Restore the state
    LOG.info("Restoring state to checkpoint id: " + request.getState().getCheckpointId());
    if (instanceState != null) {
      instanceState.clear();
      instanceState = null;
    }

    if (request.getState().hasState() && !request.getState().getState().isEmpty()) {
      @SuppressWarnings("unchecked")
      State<Serializable, Serializable> stateToRestore =
          (State<Serializable, Serializable>) serializer.deserialize(
              request.getState().getState().toByteArray());

      instanceState = stateToRestore;
    } else if (request.getState().hasStateLocation()) {
      String stateLocation = request.getState().getStateLocation();
      byte[] rawState = FileUtils.readFromFile(stateLocation);

      @SuppressWarnings("unchecked")
      State<Serializable, Serializable> stateToRestore =
          (State<Serializable, Serializable>) serializer.deserialize(rawState);
      instanceState = stateToRestore;
    } else {
      LOG.info("The restore request does not have an actual state");
    }

    // If there's no checkpoint to restore, heron provides an empty state
    if (instanceState == null) {
      instanceState = new HashMapState<>();
    }

    LOG.info("Instance state restored for checkpoint id: "
        + request.getState().getCheckpointId());

    // 1. If during the startup time. Nothing need to be done here:
    // - We would create new instance when new "PhysicalPlan" comes
    // - Instance will start either within "StartStatefulProcessing" or new "PhysicalPlan"
    // 2. If during the normal running, we need to restart the instance
    if (isInstanceStarted && helper != null) {
      LOG.info("Restarting instance");
      resetCurrentAssignment();
    }

    registerTasksWithExecutor();

    // Send back the response
    CheckpointManager.RestoreInstanceStateResponse response =
        CheckpointManager.RestoreInstanceStateResponse.newBuilder()
            .setCheckpointId(request.getState().getCheckpointId())
            .setStatus(Common.Status.newBuilder().setStatus(Common.StatusCode.OK).build())
            .build();
    streamOutCommunicator.offer(response);
  }

  private void handleNewPhysicalPlan(InstanceControlMsg instanceControlMsg) {
    PhysicalPlanHelper newHelper = instanceControlMsg.getNewPhysicalPlanHelper();

    // Bind the MetricsCollector with topologyContext
    newHelper.setTopologyContext(metricsCollector);

    if (helper == null) {
      helper = newHelper;
      handleNewAssignment();
    } else {
      TopologyAPI.TopologyState oldTopologyState = helper.getTopologyState();
      // Update the PhysicalPlanHelper
      helper = newHelper;

      instance.update(helper);

      // Handle the state changing
      if (!oldTopologyState.equals(helper.getTopologyState())) {
        switch (helper.getTopologyState()) {
          case RUNNING:
            if (!isInstanceStarted) {
              // Start the instance if it has not yet started
              startInstanceIfNeeded();
            }
            instance.activate();
            break;
          case PAUSED:
            instance.deactivate();
            break;
          default:
            throw new RuntimeException("Unexpected TopologyState is updated for spout: "
                + helper.getTopologyState());
        }
      } else {
        LOG.info("Topology state remains the same in Executor: " + oldTopologyState);
      }
    }
  }
}
