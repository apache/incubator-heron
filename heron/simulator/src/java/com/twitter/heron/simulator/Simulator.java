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

package com.twitter.heron.simulator;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.HeronTopology;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.simulator.executors.InstanceExecutor;
import com.twitter.heron.simulator.executors.MetricsExecutor;
import com.twitter.heron.simulator.executors.StreamExecutor;
import com.twitter.heron.simulator.utils.PhysicalPlanUtil;
import com.twitter.heron.spi.utils.TopologyUtils;

/**
 * One Simulator instance can only submit one topology. Please have multiple Simulator instances
 * for multiple topologies.
 */
public class Simulator {

  private static final Logger LOG = Logger.getLogger(Simulator.class.getName());
  private final List<InstanceExecutor> instanceExecutors = new LinkedList<>();

  // Thread pool to run StreamExecutor, MetricsExecutor and InstanceExecutor
  private final ExecutorService threadsPool = Executors.newCachedThreadPool();
  private SystemConfig systemConfig;
  private StreamExecutor streamExecutor;

  private MetricsExecutor metricsExecutor;

  public Simulator() {
    this(true);
  }

  public Simulator(boolean initialize) {
    if (initialize) {
      init();
    }
  }

  protected void init() {
    // Instantiate the System Config
    this.systemConfig = getSystemConfig();

    // Add the SystemConfig into SingletonRegistry. We synchronized on the singleton object here to
    // make sure the "check and register" is atomic. And wrapping the containsSingleton and
    // registerSystemConfig for easy of unit testing
    synchronized (SingletonRegistry.INSTANCE) {
      if (!isSystemConfigExisted()) {
        LOG.info("System config not existed. Registering...");
        registerSystemConfig(systemConfig);
        LOG.info("System config registered.");
      } else {
        LOG.info("System config already existed.");
      }
    }
  }

  /**
   * Check if the system config is already registered into the SingleRegistry
   *
   * @return true if it's registered; false otherwise
   */
  protected boolean isSystemConfigExisted() {
    return SingletonRegistry.INSTANCE.containsSingleton(SystemConfig.HERON_SYSTEM_CONFIG);
  }

  /**
   * Register the given system config
   */
  protected void registerSystemConfig(SystemConfig sysConfig) {
    SingletonRegistry.INSTANCE.registerSingleton(SystemConfig.HERON_SYSTEM_CONFIG, sysConfig);
  }

  public void submitTopology(String name, Config heronConfig, HeronTopology heronTopology) {
    TopologyAPI.Topology topologyToRun =
        heronTopology.
            setConfig(heronConfig).
            setName(name).
            setState(TopologyAPI.TopologyState.RUNNING).
            getTopology();

    if (!TopologyUtils.verifyTopology(topologyToRun)) {
      throw new RuntimeException("Topology object is Malformed");
    }

    PhysicalPlans.PhysicalPlan pPlan = PhysicalPlanUtil.getPhysicalPlan(topologyToRun);

    LOG.info("Physical Plan: \n" + pPlan);

    // Create the stream executor
    streamExecutor = new StreamExecutor(pPlan);

    // Create the metrics executor
    metricsExecutor = new MetricsExecutor(systemConfig);

    // Create instance Executor
    for (PhysicalPlans.Instance instance : pPlan.getInstancesList()) {
      InstanceExecutor instanceExecutor = new InstanceExecutor(pPlan, instance.getInstanceId());

      streamExecutor.addInstanceExecutor(instanceExecutor);
      metricsExecutor.addInstanceExecutor(instanceExecutor);
      instanceExecutors.add(instanceExecutor);
    }

    // Start - run executors
    // Add exception handler for any uncaught exception here.
    Thread.setDefaultUncaughtExceptionHandler(new DefaultExceptionHandler());

    threadsPool.execute(metricsExecutor);
    threadsPool.execute(streamExecutor);
    for (InstanceExecutor instanceExecutor : instanceExecutors) {
      threadsPool.execute(instanceExecutor);
    }
  }

  public void killTopology(String topologyName) {
    LOG.info("To kill topology: " + topologyName);
    stop();
    LOG.info("Topology killed successfully");
  }

  public void activate(String topologyName) {
    LOG.info("To activate topology: " + topologyName);
    for (InstanceExecutor executor : instanceExecutors) {
      executor.activate();
    }
    LOG.info("Activated topology: " + topologyName);
  }

  public void deactivate(String topologyName) {
    LOG.info("To deactivate topology: " + topologyName);
    for (InstanceExecutor executor : instanceExecutors) {
      executor.deactivate();
    }
    LOG.info("Deactivated topology:" + topologyName);
  }

  public void shutdown() {
    LOG.info("To shutdown thread pool");

    if (threadsPool.isShutdown()) {
      threadsPool.shutdownNow();
    }

    LOG.info("Heron simulator exited.");
  }

  public void stop() {
    for (InstanceExecutor executor : instanceExecutors) {
      executor.stop();
    }

    LOG.info("To stop Stream Executor");
    streamExecutor.stop();

    LOG.info("To stop Metrics Executor");
    metricsExecutor.stop();

    threadsPool.shutdown();
  }

  protected SystemConfig getSystemConfig() {
    SystemConfig sysConfig = new SystemConfig();
    sysConfig.put(SystemConfig.INSTANCE_SET_DATA_TUPLE_CAPACITY, 256);
    sysConfig.put(SystemConfig.INSTANCE_SET_CONTROL_TUPLE_CAPACITY, 256);
    sysConfig.put(SystemConfig.HERON_METRICS_EXPORT_INTERVAL_SEC, 60);
    sysConfig.put(SystemConfig.INSTANCE_EXECUTE_BATCH_TIME_MS, 16);
    sysConfig.put(SystemConfig.INSTANCE_EXECUTE_BATCH_SIZE_BYTES, 32768);
    sysConfig.put(SystemConfig.INSTANCE_EMIT_BATCH_TIME_MS, 16);
    sysConfig.put(SystemConfig.INSTANCE_EMIT_BATCH_SIZE_BYTES, 32768);
    sysConfig.put(SystemConfig.INSTANCE_ACK_BATCH_TIME_MS, 128);
    sysConfig.put(SystemConfig.INSTANCE_ACKNOWLEDGEMENT_NBUCKETS, 10);

    return sysConfig;
  }

  /**
   * Handler for catching exceptions thrown by any threads (owned either by topology or heron
   * infrastructure).
   * Will flush all attached log handler and close them.
   * Attempt to flush all the connection.
   * Terminate the JVM.
   */
  public class DefaultExceptionHandler implements Thread.UncaughtExceptionHandler {
    public void uncaughtException(Thread thread, Throwable exception) {
      // Add try and catch block to prevent new exceptions stop the handling thread
      try {
        // Delegate to the actual one
        handleException(thread, exception);

        // SUPPRESS CHECKSTYLE IllegalCatch
      } catch (Throwable t) {
        LOG.log(Level.SEVERE, "Failed to handle exception. Process halting", t);
        Runtime.getRuntime().halt(1);
      }
    }

    // The actual uncaught exceptions handing logic
    private void handleException(Thread thread, Throwable exception) {
      LOG.severe("Local Mode Process exiting.");
      LOG.log(Level.SEVERE,
          "Exception caught in thread: " + thread.getName() + " with id: " + thread.getId(),
          exception);
      for (Handler handler : java.util.logging.Logger.getLogger("").getHandlers()) {
        handler.close();
      }

      // Attempts to shutdown all the thread in threadsPool. This will send Interrupt to every
      // thread in the pool. Threads may implement a clean Interrupt logic.
      threadsPool.shutdownNow();

      // not owned by HeronInstance). To be safe, not sending these interrupts.
      Runtime.getRuntime().halt(1);
    }
  }
}
