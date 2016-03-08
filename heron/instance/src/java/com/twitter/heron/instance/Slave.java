package com.twitter.heron.instance;

import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.utils.metrics.MetricsCollector;
import com.twitter.heron.common.utils.misc.Constants;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.instance.bolt.BoltInstance;
import com.twitter.heron.instance.spout.SpoutInstance;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.proto.system.Metrics;

/**
 * The slave, which in fact is a InstanceFactory, will new a spout or bolt according to the PhysicalPlan.
 * First, if the instance is null, it will wait for the PhysicalPlan from inQueue and, if it receives one,
 * we will instantiate a new instance (spout or bolt) according to the PhysicalPlanHelper in SingletonRegistry.
 * It is a Runnable so it could be executed in a Thread. During run(), it will begin the SlaveLooper's loop().
 */

public class Slave implements Runnable {
  private static final Logger LOG = Logger.getLogger(Slave.class.getName());

  private final SlaveLooper slaveLooper;
  private final MetricsCollector metricsCollector;

  private IInstance instance;

  private PhysicalPlanHelper helper;

  // Communicator
  private final Communicator<HeronTuples.HeronTupleSet> streamInCommunicator;
  private final Communicator<HeronTuples.HeronTupleSet> streamOutCommunicator;
  private final Communicator<InstanceControlMsg> inControlQueue;

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

            if (helper == null) {
              handleNewAssignment(newHelper);
            } else {
              // Handle the state changing
              if (!helper.getTopologyState().equals(newHelper.getTopologyState())) {
                if (newHelper.getTopologyState().equals(TopologyAPI.TopologyState.RUNNING)) {
                  if (!isInstanceStarted) {
                    // Start the instance if it has not yet started
                    startInstance();
                  }
                  instance.activate();
                } else if (newHelper.getTopologyState().equals(TopologyAPI.TopologyState.PAUSED)) {
                  instance.deactivate();
                } else {
                  throw new RuntimeException("Unexpected TopologyState is updated for spout: "
                      + newHelper.getTopologyState());
                }
              } else {
                LOG.info("Topology state remains the same in Slave.");
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
    LOG.info("Incarnating ourselves as " +
        newHelper.getMyComponent() + " with task id " +
        newHelper.getMyTaskId());

    // Bind the MetricsCollector with topologyContext
    newHelper.setTopologyContext(metricsCollector);

    // During the initiation of instance, we would add a bunch of tasks to slaveLooper's tasksOnWakeup
    if (newHelper.getMySpout() != null) {
      instance =
          new SpoutInstance(newHelper,
              streamInCommunicator,
              streamOutCommunicator,
              slaveLooper);

      streamInCommunicator.init(systemConfig.getInstanceInternalSpoutReadQueueCapacity(),
          systemConfig.getInstanceTuningExpectedSpoutReadQueueSize(),
          systemConfig.getInstanceTuningCurrentSampleWeight());
      streamOutCommunicator.init(systemConfig.getInstanceInternalSpoutWriteQueueCapacity(),
          systemConfig.getInstanceTuningExpectedSpoutWriteQueueSize(),
          systemConfig.getInstanceTuningCurrentSampleWeight());
    } else {
      instance =
          new BoltInstance(newHelper,
              streamInCommunicator,
              streamOutCommunicator,
              slaveLooper);

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
    Thread.currentThread().setName(Constants.THREAD_SLAVE_NAME);

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
    instance.stop();
  }
}
