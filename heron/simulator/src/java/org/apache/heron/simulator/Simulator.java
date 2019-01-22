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

package org.apache.heron.simulator;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.heron.api.Config;
import org.apache.heron.api.HeronTopology;
import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.utils.TopologyUtils;
import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.config.SystemConfigKey;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.simulator.executors.InstanceExecutor;
import org.apache.heron.simulator.executors.MetricsExecutor;
import org.apache.heron.simulator.executors.StreamExecutor;
import org.apache.heron.simulator.utils.TopologyManager;

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

  /**
   * Submit and run topology in simulator
   * @param name topology name
   * @param heronConfig topology config
   * @param heronTopology topology built from topology builder
   */
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

    // TODO (nlu): add simulator support stateful processing
    if (isTopologyStateful(heronConfig)) {
      throw new RuntimeException("Stateful topology is not supported");
    }

    TopologyManager topologyManager = new TopologyManager(topologyToRun);

    LOG.info("Physical Plan: \n" + topologyManager.getPhysicalPlan());

    // Create the stream executor
    streamExecutor = new StreamExecutor(topologyManager);

    // Create the metrics executor
    metricsExecutor = new MetricsExecutor(systemConfig);

    // Create instance Executor
    for (PhysicalPlans.Instance instance : topologyManager.getPhysicalPlan().getInstancesList()) {
      InstanceExecutor instanceExecutor = new InstanceExecutor(
          topologyManager.getPhysicalPlan(),
          instance.getInstanceId()
      );

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
    SystemConfig.Builder builder = SystemConfig.newBuilder(true)
        .put(SystemConfigKey.INSTANCE_SET_DATA_TUPLE_CAPACITY, 256)
        .put(SystemConfigKey.INSTANCE_SET_CONTROL_TUPLE_CAPACITY, 256)
        .put(SystemConfigKey.HERON_METRICS_EXPORT_INTERVAL, 60)
        .put(SystemConfigKey.INSTANCE_EXECUTE_BATCH_TIME, 16)
        .put(SystemConfigKey.INSTANCE_EXECUTE_BATCH_SIZE, ByteAmount.fromBytes(32768))
        .put(SystemConfigKey.INSTANCE_EMIT_BATCH_TIME, 16)
        .put(SystemConfigKey.INSTANCE_EMIT_BATCH_SIZE, ByteAmount.fromBytes(32768))
        .put(SystemConfigKey.INSTANCE_ACK_BATCH_TIME, 128)
        .put(SystemConfigKey.INSTANCE_ACKNOWLEDGEMENT_NBUCKETS, 10);

    return builder.build();
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

  private boolean isTopologyStateful(Config heronConfig) {
    Config.TopologyReliabilityMode mode =
        Config.TopologyReliabilityMode.valueOf(
            String.valueOf(heronConfig.get(Config.TOPOLOGY_RELIABILITY_MODE)));

    return Config.TopologyReliabilityMode.EFFECTIVELY_ONCE.equals(mode);
  }
}
