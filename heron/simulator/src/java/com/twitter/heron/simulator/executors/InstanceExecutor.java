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

import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.utils.metrics.MetricsCollector;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.proto.system.Metrics;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.simulator.instance.BoltInstance;
import com.twitter.heron.simulator.instance.IInstance;
import com.twitter.heron.simulator.instance.SpoutInstance;

/**
 * InstanceExecutor helps to group all necessary resources for an instance into a class and,
 * provide methods to access these resources externally.
 * <p>
 * It helps uniform the ways to access instance resources
 */
public class InstanceExecutor implements Runnable {
  public static final int CAPACITY = 5;
  public static final double CURRENT_SAMPLE_WEIGHT = 0.5;

  private static final Logger LOG = Logger.getLogger(InstanceExecutor.class.getName());

  private final PhysicalPlanHelper physicalPlanHelper;

  private final SlaveLooper looper;

  private final Communicator<HeronTuples.HeronTupleSet> streamInQueue;
  private final Communicator<HeronTuples.HeronTupleSet> streamOutQueue;
  private final Communicator<Metrics.MetricPublisherPublishMessage> metricsOutQueue;

  private IInstance instance;

  private volatile boolean toStop = false;
  private volatile boolean toActivate = false;
  private volatile boolean toDeactivate = false;

  private boolean isInstanceStarted = false;

  public InstanceExecutor(PhysicalPlans.PhysicalPlan physicalPlan,
                          String instanceId) {
    streamInQueue = new Communicator<>();
    streamOutQueue = new Communicator<>();
    metricsOutQueue = new Communicator<>();
    looper = new SlaveLooper();

    MetricsCollector metricsCollector = new MetricsCollector(looper, metricsOutQueue);

    physicalPlanHelper = createPhysicalPlanHelper(physicalPlan, instanceId, metricsCollector);

    initInstanceManager();

    LOG.log(Level.INFO, "Incarnating ourselves as {0} with task id {1}",
        new Object[]{physicalPlanHelper.getMyComponent(), physicalPlanHelper.getMyTaskId()});
  }

  public Communicator<HeronTuples.HeronTupleSet> getStreamInQueue() {
    return streamInQueue;
  }

  public Communicator<HeronTuples.HeronTupleSet> getStreamOutQueue() {
    return streamOutQueue;
  }

  public Communicator<Metrics.MetricPublisherPublishMessage> getMetricsOutQueue() {
    return metricsOutQueue;
  }

  public String getInstanceId() {
    return physicalPlanHelper.getMyInstanceId();
  }

  public String getComponentName() {
    return physicalPlanHelper.getMyComponent();
  }

  public int getTaskId() {
    return physicalPlanHelper.getMyTaskId();
  }

  protected IInstance createInstance() {
    return (physicalPlanHelper.getMySpout() != null)
        ? new SpoutInstance(physicalPlanHelper, streamInQueue, streamOutQueue, looper)
        : new BoltInstance(physicalPlanHelper, streamInQueue, streamOutQueue, looper);
  }

  protected PhysicalPlanHelper createPhysicalPlanHelper(PhysicalPlans.PhysicalPlan physicalPlan,
                                                        String instanceId,
                                                        MetricsCollector metricsCollector) {
    PhysicalPlanHelper localPhysicalPlanHelper = new PhysicalPlanHelper(physicalPlan, instanceId);

    // Bind the MetricsCollector with topologyContext
    localPhysicalPlanHelper.setTopologyContext(metricsCollector);

    return localPhysicalPlanHelper;
  }

  protected void initInstanceManager() {
    streamInQueue.setConsumer(looper);
    streamInQueue.init(CAPACITY,
        CAPACITY,
        CURRENT_SAMPLE_WEIGHT);

    streamOutQueue.setProducer(looper);
    streamOutQueue.init(CAPACITY,
        CAPACITY,
        CURRENT_SAMPLE_WEIGHT);

    metricsOutQueue.setProducer(looper);
  }

  // Flags would be set in other threads,
  // But we have to handle these flags inside the WakeableLooper thread
  protected void handleControlSignal() {
    if (toActivate) {
      if (!isInstanceStarted) {
        startInstance();
      }

      instance.activate();
      LOG.info("Activated instance: " + physicalPlanHelper.getMyInstanceId());

      // Reset the flag value
      toActivate = false;
    }

    if (toDeactivate) {
      instance.deactivate();
      LOG.info("Deactivated instance: " + physicalPlanHelper.getMyInstanceId());

      // Reset the flag value
      toDeactivate = false;
    }

    if (toStop) {
      instance.stop();
      LOG.info("Stopped instance: " + physicalPlanHelper.getMyInstanceId());

      // Reset the flag value
      toStop = false;

      return;
    }
  }

  @Override
  public void run() {
    Thread.currentThread().setName(String.format("%s_%s",
        physicalPlanHelper.getMyComponent(),
        physicalPlanHelper.getMyInstanceId()));

    instance = createInstance();

    if (physicalPlanHelper.getTopologyState().equals(TopologyAPI.TopologyState.RUNNING)) {
      startInstance();
    }

    // Handle Control Signal if needed
    Runnable handleControlTask = new Runnable() {
      @Override
      public void run() {
        handleControlSignal();
      }
    };
    looper.addTasksOnWakeup(handleControlTask);

    looper.loop();
  }

  public void stop() {
    toStop = true;
    looper.wakeUp();
  }

  public void activate() {
    toActivate = true;
    looper.wakeUp();
  }

  public void deactivate() {
    toDeactivate = true;
    looper.wakeUp();
  }

  private void startInstance() {
    instance.start();
    isInstanceStarted = true;
    LOG.info("Started instance.");
  }
}
