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

import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.utils.metrics.MetricsCollector;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.common.utils.misc.ThreadNames;
import com.twitter.heron.instance.bolt.BoltInstance;
import com.twitter.heron.instance.spout.SpoutInstance;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.proto.system.Metrics;

/**
 * The slave, which in fact is a InstanceFactory, creates a new spout or bolt according to the PhysicalPlan.
 * First, if the instance is null, it will wait for the PhysicalPlan from inQueue and, if it receives one,
 * will instantiate a new instance (spout or bolt) according to the PhysicalPlanHelper in SingletonRegistry.
 * It is a Runnable so it could be executed in a Thread. During run(), it will begin the SlaveLooper's loop().
 */

public class Slave implements Runnable, AutoCloseable {
  private static final Logger LOG = Logger.getLogger(Slave.class.getName());

  private final SlaveLooper slaveLooper;
  private final MetricsCollector metricsCollector;
  // Communicator
  private final Communicator<HeronTuples.HeronTupleSet> streamInCommunicator;
  private final Communicator<HeronTuples.HeronTupleSet> streamOutCommunicator;
  private final Communicator<InstanceControlMsg> inControlQueue;
  private IInstance instance;
  private PhysicalPlanHelper helper;
  private SystemConfig systemConfig;

  private boolean isInstanceStarted = false;

  public Slave(SlaveLooper slaveLooper,
               final Communicator<HeronTuples.HeronTupleSet> streamInCommunicator,
               final Communicator<HeronTuples.HeronTupleSet> streamOutCommunicator,
               final Communicator<InstanceControlMsg> inControlQueue,
               final Communicator<Metrics.MetricPublisherPublishMessage> metricsOutCommunicator) {
    this.slaveLooper = slaveLooper;
    this.streamInCommunicator = streamInCommunicator;
    this.streamOutCommunicator = streamOutCommunicator;
    this.inControlQueue = inControlQueue;

    this.systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    this.metricsCollector = new MetricsCollector(slaveLooper, metricsOutCommunicator);

    handleControlMessage();
  }

  private void handleControlMessage() {
    Runnable handleControlMessageTask = new Runnable() {
      @Override
      public void run() {
        while (!inControlQueue.isEmpty()) {
          InstanceControlMsg instanceControlMsg = inControlQueue.poll();

          // Handle New Physical Plan
          if (instanceControlMsg.isNewPhysicalPlanHelper()) {
            PhysicalPlanHelper newHelper = instanceControlMsg.getNewPhysicalPlanHelper();

            // Bind the MetricsCollector with topologyContext
            newHelper.setTopologyContext(metricsCollector);

            if (helper == null) {
              handleNewAssignment(newHelper);
            } else {

              instance.update(newHelper);

              // Handle the state changing
              if (!helper.getTopologyState().equals(newHelper.getTopologyState())) {
                switch (newHelper.getTopologyState()) {
                  case RUNNING:
                    if (!isInstanceStarted) {
                      // Start the instance if it has not yet started
                      startInstance();
                    }
                    instance.activate();
                    break;
                  case PAUSED:
                    instance.deactivate();
                    break;
                  default:
                    throw new RuntimeException("Unexpected TopologyState is updated for spout: "
                        + newHelper.getTopologyState());
                }
              } else {
                LOG.info("Topology state remains the same in Slave: " + helper.getTopologyState());
              }
            }

            // update the PhysicalPlanHelper in Slave
            helper = newHelper;
          }


          // TODO:- We might handle more control Message in future
        }
      }
    };

    slaveLooper.addTasksOnWakeup(handleControlMessageTask);
  }

  private void handleNewAssignment(PhysicalPlanHelper newHelper) {
    LOG.log(Level.INFO,
        "Incarnating ourselves as {0} with task id {1}",
        new Object[]{newHelper.getMyComponent(), newHelper.getMyTaskId()});

    // During the initiation of instance,
    // we would add a bunch of tasks to slaveLooper's tasksOnWakeup
    if (newHelper.getMySpout() != null) {
      instance =
          new SpoutInstance(newHelper, streamInCommunicator, streamOutCommunicator, slaveLooper);

      streamInCommunicator.init(systemConfig.getInstanceInternalSpoutReadQueueCapacity(),
          systemConfig.getInstanceTuningExpectedSpoutReadQueueSize(),
          systemConfig.getInstanceTuningCurrentSampleWeight());
      streamOutCommunicator.init(systemConfig.getInstanceInternalSpoutWriteQueueCapacity(),
          systemConfig.getInstanceTuningExpectedSpoutWriteQueueSize(),
          systemConfig.getInstanceTuningCurrentSampleWeight());
    } else {
      instance =
          new BoltInstance(newHelper, streamInCommunicator, streamOutCommunicator, slaveLooper);

      streamInCommunicator.init(systemConfig.getInstanceInternalBoltReadQueueCapacity(),
          systemConfig.getInstanceTuningExpectedBoltReadQueueSize(),
          systemConfig.getInstanceTuningCurrentSampleWeight());
      streamOutCommunicator.init(systemConfig.getInstanceInternalBoltWriteQueueCapacity(),
          systemConfig.getInstanceTuningExpectedBoltWriteQueueSize(),
          systemConfig.getInstanceTuningCurrentSampleWeight());
    }

    if (newHelper.getTopologyState().equals(TopologyAPI.TopologyState.RUNNING)) {
      // We would start the instance only if the TopologyState is RUNNING
      startInstance();
    } else {
      LOG.info("The instance is deployed in deactivated state");
    }
  }

  @Override
  public void run() {
    Thread.currentThread().setName(ThreadNames.THREAD_SLAVE_NAME);

    slaveLooper.loop();
  }

  private void startInstance() {
    instance.start();
    isInstanceStarted = true;
    LOG.info("Started instance.");
  }

  public void close() {
    LOG.info("Closing the Slave Thread");
    this.metricsCollector.forceGatherAllMetrics();
    LOG.info("Cleaning up the instance");
    if (instance != null) {
      instance.stop();
    }
  }
}
