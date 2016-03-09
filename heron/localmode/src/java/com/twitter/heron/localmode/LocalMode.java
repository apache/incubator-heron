package com.twitter.heron.localmode;

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
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.localmode.executors.InstanceExecutor;
import com.twitter.heron.localmode.executors.MetricsExecutor;
import com.twitter.heron.localmode.executors.StreamExecutor;
import com.twitter.heron.localmode.utils.PhysicalPlanUtil;
import com.twitter.heron.proto.system.PhysicalPlans;


/**
 * One LocalMode instance can only submit one topology. Please have multiple LocalMode instances
 * for multiple topologies.
 */
public class LocalMode {
  private static final Logger LOG = Logger.getLogger(LocalMode.class.getName());

  private SystemConfig systemConfig;

  private final List<InstanceExecutor> instanceExecutors = new LinkedList<>();

  // Thread pool to run StreamExecutor, MetricsExecutor and InstanceExecutor
  private final ExecutorService threadsPool = Executors.newCachedThreadPool();

  private StreamExecutor streamExecutor;

  private MetricsExecutor metricsExecutor;

  public LocalMode() {
    this(true);
  }

  public LocalMode(boolean initialize) {
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
   * @return true if it's registered; false otherwise
   */
  protected boolean isSystemConfigExisted() {
    return SingletonRegistry.INSTANCE.containsSingleton(SystemConfig.HERON_SYSTEM_CONFIG);
  }

  /**
   * Register the given system config
   * @param systemConfig
   */
  protected void registerSystemConfig(SystemConfig systemConfig) {
    SingletonRegistry.INSTANCE.registerSingleton(SystemConfig.HERON_SYSTEM_CONFIG, systemConfig);
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

      // TODO : It is not clear if this signal should be sent to all the threads (including threads
      // not owned by HeronInstance). To be safe, not sending these interrupts.
      Runtime.getRuntime().halt(1);
    }
  }

  public void submitTopology(String name, Config heronConfig, HeronTopology heronTopology) {
    TopologyAPI.Topology topologyToRun =
        heronTopology.
            setConfig(heronConfig).
            setName(name).
            setState(TopologyAPI.TopologyState.RUNNING).
            getTopology();

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

    LOG.info("Local mode exited.");
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
    SystemConfig systemConfig = new SystemConfig();
    systemConfig.put(SystemConfig.INSTANCE_SET_DATA_TUPLE_CAPACITY, 256);
    systemConfig.put(SystemConfig.INSTANCE_SET_CONTROL_TUPLE_CAPACITY, 256);
    systemConfig.put(SystemConfig.HERON_METRICS_EXPORT_INTERVAL_SEC, 60);
    systemConfig.put(SystemConfig.INSTANCE_EXECUTE_BATCH_TIME_MS, 16);
    systemConfig.put(SystemConfig.INSTANCE_EXECUTE_BATCH_SIZE_BYTES, 32768);
    systemConfig.put(SystemConfig.INSTANCE_EMIT_BATCH_TIME_MS, 16);
    systemConfig.put(SystemConfig.INSTANCE_EMIT_BATCH_SIZE_BYTES, 32768);
    systemConfig.put(SystemConfig.INSTANCE_ACK_BATCH_TIME_MS, 128);
    systemConfig.put(SystemConfig.INSTANCE_ACKNOWLEDGEMENT_NBUCKETS, 10);

    return systemConfig;
  }
}
