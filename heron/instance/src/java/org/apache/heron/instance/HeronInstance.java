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

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.ExecutorLooper;
import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.utils.logging.ErrorReportLoggingHandler;
import org.apache.heron.common.utils.logging.LoggingHelper;
import org.apache.heron.common.utils.misc.ThreadNames;
import org.apache.heron.proto.system.Metrics;
import org.apache.heron.proto.system.PhysicalPlans;

/**
 * HeronInstance is just an entry with resource initiation.
 */

public class HeronInstance {
  private static final Logger LOG = Logger.getLogger(HeronInstance.class.getName());

  private static final int NUM_THREADS = 2;

  private final NIOLooper gatewayLooper;
  private final ExecutorLooper executorLooper;

  // Only one outStreamQueue, which is responsible for both control tuples and data tuples
  private final Communicator<Message> outStreamQueue;

  // This blocking queue is used to buffer tuples read from socket and ready to be used by instance
  // For spout, it will buffer Control tuple, while for bolt, it will buffer data tuple.
  private final Communicator<Message> inStreamQueue;

  // This queue is used to pass Control Message from Gateway to Executor
  // TODO:- currently it would just pass the PhysicalPlanHelper
  // TODO:- we might handle more types of ControlMessage in future
  private final Communicator<InstanceControlMsg> inControlQueue;

  // This blocking queue is used to buffer the metrics info ready to send out to metrics manager
  // The 0th queue will be used to buffer the system metrics collected by Gateway
  // The 1th queue will be used to buffer the instance metrics collected by Instance
  private final List<Communicator<Metrics.MetricPublisherPublishMessage>> outMetricsQueues;

  private final Gateway gateway;
  private final Executor executor;

  private final ExecutorService threadsPool;

  private final SystemConfig systemConfig;

  private static class CommandLineOptions {
    private static final String TOPOLOGY_NAME_OPTION = "topology_name";
    private static final String TOPOLOGY_ID_OPTION = "topology_id";
    private static final String INSTANCE_ID_OPTION = "instance_id";
    private static final String COMPONENT_NAME_OPTION = "component_name";
    private static final String TASK_ID_OPTION = "task_id";
    private static final String COMPONENT_INDEX_OPTION = "component_index";
    private static final String STMGR_ID_OPTION = "stmgr_id";
    private static final String STMGR_PORT_OPTION = "stmgr_port";
    private static final String METRICS_MGR_PORT_OPTION = "metricsmgr_port";
    private static final String SYSTEM_CONFIG_FILE = "system_config_file";
    private static final String OVERRIDE_CONFIG_FILE = "override_config_file";
    private static final String REMOTE_DEBUGGER_PORT = "remote_debugger_port";
  }

  /**
   * Heron instance constructor
   */
  public HeronInstance(String topologyName, String topologyId,
                       PhysicalPlans.Instance instance, int streamPort, int metricsPort)
      throws IOException {
    systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    // Two WakeableLooper
    gatewayLooper = new NIOLooper();
    executorLooper = new ExecutorLooper();

    // Add the task on exit
    gatewayLooper.addTasksOnExit(new GatewayExitTask());
    executorLooper.addTasksOnExit(new ExecutorExitTask());

    // For stream
    inStreamQueue = new Communicator<Message>(gatewayLooper, executorLooper);
    outStreamQueue = new Communicator<Message>(executorLooper, gatewayLooper);
    inControlQueue = new Communicator<InstanceControlMsg>(gatewayLooper, executorLooper);

    // Now for metrics
    // No need in queues for metrics
    outMetricsQueues = new ArrayList<Communicator<Metrics.MetricPublisherPublishMessage>>(2);

    Communicator<Metrics.MetricPublisherPublishMessage> gatewayMetricsOut =
        new Communicator<Metrics.MetricPublisherPublishMessage>(gatewayLooper, gatewayLooper);
    gatewayMetricsOut.init(systemConfig.getInstanceInternalMetricsWriteQueueCapacity(),
        systemConfig.getInstanceTuningExpectedMetricsWriteQueueSize(),
        systemConfig.getInstanceTuningCurrentSampleWeight());

    Communicator<Metrics.MetricPublisherPublishMessage> executorMetricsOut =
        new Communicator<Metrics.MetricPublisherPublishMessage>(executorLooper, gatewayLooper);
    executorMetricsOut.init(systemConfig.getInstanceInternalMetricsWriteQueueCapacity(),
        systemConfig.getInstanceTuningExpectedMetricsWriteQueueSize(),
        systemConfig.getInstanceTuningCurrentSampleWeight());

    outMetricsQueues.add(gatewayMetricsOut);
    outMetricsQueues.add(executorMetricsOut);

    // We will new these two Runnable
    this.gateway =
        new Gateway(topologyName, topologyId, instance, streamPort, metricsPort,
            gatewayLooper, inStreamQueue, outStreamQueue, inControlQueue, outMetricsQueues);
    this.executor = new Executor(executorLooper, inStreamQueue, outStreamQueue,
        inControlQueue, executorMetricsOut);

    // New the ThreadPool and register it inside the SingletonRegistry
    threadsPool = Executors.newFixedThreadPool(NUM_THREADS);
  }

  private static CommandLine parseCommandLineArgs(String[] args) {
    Options options = new Options();

    Option topologyNameOption = new Option(
        CommandLineOptions.TOPOLOGY_NAME_OPTION, true, "Topology Name");
    topologyNameOption.setRequired(true);
    topologyNameOption.setType(String.class);
    options.addOption(topologyNameOption);

    Option topologyIdOption = new Option(
        CommandLineOptions.TOPOLOGY_ID_OPTION, true, "Topology ID");
    topologyIdOption.setRequired(true);
    topologyIdOption.setType(String.class);
    options.addOption(topologyIdOption);

    Option instanceIdOption = new Option(
        CommandLineOptions.INSTANCE_ID_OPTION, true, "Instance ID");
    instanceIdOption.setRequired(true);
    instanceIdOption.setType(String.class);
    options.addOption(instanceIdOption);

    Option componentNameOption = new Option(
        CommandLineOptions.COMPONENT_NAME_OPTION, true, "Component Name");
    componentNameOption.setRequired(true);
    componentNameOption.setType(String.class);
    options.addOption(componentNameOption);

    Option taskIdOption = new Option(CommandLineOptions.TASK_ID_OPTION, true, "Task ID");
    taskIdOption.setRequired(true);
    taskIdOption.setType(Integer.class);
    options.addOption(taskIdOption);

    Option componentIndexOption = new Option(
        CommandLineOptions.COMPONENT_INDEX_OPTION, true, "Component Index");
    componentIndexOption.setRequired(true);
    componentIndexOption.setType(Integer.class);
    options.addOption(componentIndexOption);

    Option stmgrIdOption = new Option(
        CommandLineOptions.STMGR_ID_OPTION, true, "Stream Manager ID");
    stmgrIdOption.setType(String.class);
    stmgrIdOption.setRequired(true);
    options.addOption(stmgrIdOption);

    Option stmgrPortOption = new Option(
        CommandLineOptions.STMGR_PORT_OPTION, true, "Stream Manager Port");
    stmgrPortOption.setType(Integer.class);
    stmgrPortOption.setRequired(true);
    options.addOption(stmgrPortOption);

    Option metricsmgrPortOption = new Option(
        CommandLineOptions.METRICS_MGR_PORT_OPTION, true, "Metrics Manager Port");
    metricsmgrPortOption.setType(Integer.class);
    metricsmgrPortOption.setRequired(true);
    options.addOption(metricsmgrPortOption);

    Option systemConfigFileOption = new Option(
        CommandLineOptions.SYSTEM_CONFIG_FILE, true, "Heron Internals Config Filename");
    systemConfigFileOption.setType(String.class);
    systemConfigFileOption.setRequired(true);
    options.addOption(systemConfigFileOption);

    Option overrideConfigFileOption
        = new Option(CommandLineOptions.OVERRIDE_CONFIG_FILE,
        true, "Override Config File");
    overrideConfigFileOption.setType(String.class);
    overrideConfigFileOption.setRequired(true);
    options.addOption(overrideConfigFileOption);

    Option remoteDebuggerPortOption = new Option(
        CommandLineOptions.REMOTE_DEBUGGER_PORT, true, "Remote Debugger Port");
    remoteDebuggerPortOption.setType(Integer.class);
    options.addOption(remoteDebuggerPortOption);

    CommandLineParser parser = new DefaultParser();
    HelpFormatter formatter = new HelpFormatter();
    CommandLine cmd = null;
    try {
      cmd = parser.parse(options, args);
    } catch (ParseException e) {
      System.out.println(e.getMessage());
      formatter.printHelp("Heron Instance", options);
      throw new RuntimeException("Incorrect Usage");
    }
    return cmd;
  }

  public static void main(String[] args) throws IOException {
    CommandLine commandLine = parseCommandLineArgs(args);

    String topologyName = commandLine.getOptionValue(CommandLineOptions.TOPOLOGY_NAME_OPTION);
    String topologyId = commandLine.getOptionValue(CommandLineOptions.TOPOLOGY_ID_OPTION);
    String instanceId = commandLine.getOptionValue(CommandLineOptions.INSTANCE_ID_OPTION);
    String componentName = commandLine.getOptionValue(CommandLineOptions.COMPONENT_NAME_OPTION);
    Integer taskId = Integer.parseInt(
        commandLine.getOptionValue(CommandLineOptions.TASK_ID_OPTION));
    Integer componentIndex
        = Integer.parseInt(commandLine.getOptionValue(CommandLineOptions.COMPONENT_INDEX_OPTION));
    String streamId = commandLine.getOptionValue(CommandLineOptions.STMGR_ID_OPTION);
    Integer streamPort
        = Integer.parseInt(commandLine.getOptionValue(CommandLineOptions.STMGR_PORT_OPTION));
    Integer metricsPort
        = Integer.parseInt(
        commandLine.getOptionValue(CommandLineOptions.METRICS_MGR_PORT_OPTION));
    String systemConfigFile
        = commandLine.getOptionValue(CommandLineOptions.SYSTEM_CONFIG_FILE);
    String overrideConfigFile
        = commandLine.getOptionValue(CommandLineOptions.OVERRIDE_CONFIG_FILE);

    Integer remoteDebuggerPort = null;
    if (commandLine.hasOption(CommandLineOptions.REMOTE_DEBUGGER_PORT)) {
      remoteDebuggerPort = Integer.parseInt(
          commandLine.getOptionValue(CommandLineOptions.REMOTE_DEBUGGER_PORT));
    }

    SystemConfig systemConfig = SystemConfig.newBuilder(true)
        .putAll(systemConfigFile, true)
        .putAll(overrideConfigFile, true)
        .build();

    // Add the SystemConfig into SingletonRegistry
    SingletonRegistry.INSTANCE.registerSingleton(SystemConfig.HERON_SYSTEM_CONFIG, systemConfig);

    // Create the protobuf Instance
    PhysicalPlans.InstanceInfo.Builder instanceInfoBuilder
        = PhysicalPlans.InstanceInfo.newBuilder().setTaskId(taskId)
        .setComponentIndex(componentIndex).setComponentName(componentName);
    if (remoteDebuggerPort != null) {
      instanceInfoBuilder.setRemoteDebuggerPort(remoteDebuggerPort);
    }
    PhysicalPlans.InstanceInfo instanceInfo = instanceInfoBuilder.build();

    PhysicalPlans.Instance instance = PhysicalPlans.Instance.newBuilder().
        setInstanceId(instanceId).setStmgrId(streamId).setInfo(instanceInfo).build();

    // Init the logging setting and redirect the stdout and stderr to logging
    // For now we just set the logging level as INFO; later we may accept an argument to set it.
    Level loggingLevel = Level.INFO;
    String loggingDir = systemConfig.getHeronLoggingDirectory();

    // Log to file and TManager
    LoggingHelper.loggerInit(loggingLevel, true);
    LoggingHelper.addLoggingHandler(
        LoggingHelper.getFileHandler(instanceId, loggingDir, true,
            systemConfig.getHeronLoggingMaximumSize(),
            systemConfig.getHeronLoggingMaximumFiles()));
    LoggingHelper.addLoggingHandler(new ErrorReportLoggingHandler());

    String logMsg = "\nStarting instance " + instanceId + " for topology " + topologyName
        + " and topologyId " + topologyId + " for component " + componentName
        + " with taskId " + taskId + " and componentIndex " + componentIndex
        + " and streamManagerId " + streamId + " and streamManagerPort " + streamPort
        + " and metricsManagerPort " + metricsPort;

    if (remoteDebuggerPort != null) {
      logMsg += " and remoteDebuggerPort " + remoteDebuggerPort;
    }

    LOG.info(logMsg);

    LOG.info("System Config: " + systemConfig.toString());

    HeronInstance heronInstance =
        new HeronInstance(topologyName, topologyId, instance, streamPort, metricsPort);
    heronInstance.start();
  }

  public void start() {
    // Add exception handler for any uncaught exception here.
    Thread.setDefaultUncaughtExceptionHandler(new DefaultExceptionHandler());

    // Get the Thread Pool and run it
    threadsPool.execute(gateway);
    threadsPool.execute(executor);
  }

  public void stop() {
    synchronized (this) {
      LOG.severe("Instance Process exiting.\n");
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

  /**
   * Handler for catching exceptions thrown by any threads (owned either by topology or heron
   * infrastructure).
   * 1. Will flush all attached log handler and close them.
   * 2. Attempt to flush all the connection.
   * 3. Terminate the JVM.
   * 4. The process would be forced exiting if exceeding timeout
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
      // We would fail fast when errors occur to avoid unexpected bad situations
      if (exception instanceof Error) {
        try {
          LOG.log(Level.SEVERE, "Error caught in thread: " + thread.getName()
                + " with thread id: " + thread.getId() + ". Process halting...", exception);
        } finally {
          // If an OOM happens, it is likely that logging above will
          // cause another OOM.
          Runtime.getRuntime().halt(1);
        }
      }

      LOG.log(Level.SEVERE,
          String.format("Exception caught in thread: %s with id: %d",
              thread.getName(), thread.getId()), exception);

      // CountDownLatch to notify ForceExitTask whether exit is done
      final CountDownLatch exited = new CountDownLatch(1);
      final ExecutorService exitExecutor = Executors.newSingleThreadExecutor();
      exitExecutor.execute(new ForceExitTask(exited, systemConfig.getInstanceForceExitTimeout()));

      // Clean up
      if (thread.getName().equals(ThreadNames.THREAD_EXECUTOR_NAME)) {
        // Run the ExecutorExitTask here since the thread throw exceptions
        // and this Task would never be invoked on exit in future
        new ExecutorExitTask().run();

        // And exit the GatewayLooper
        gatewayLooper.exitLoop();
      } else {
        // If the exceptions happen in other threads
        // We would just invoke GatewayExitTask
        new GatewayExitTask().run();
      }

      // This notifies the ForceExitTask that the task is finished so that
      // it is not halted forcibly.
      exited.countDown();
    }
  }

  // The Task to execute on Gateway thread's exit
  public class GatewayExitTask implements Runnable {

    @Override
    public void run() {
      SysUtils.closeIgnoringExceptions(gateway);

      stop();
    }
  }

  // The Task to execute on Executor thread's exit
  public class ExecutorExitTask implements Runnable {

    @Override
    public void run() {
      SysUtils.closeIgnoringExceptions(executor);
    }
  }

  // The task is to guarantee this process would exit in certain timeout
  // It would receive a CountDownLatch as a notifier and a timeout value in long.
  // If the CountDownLatch does not be 0 in the timeout,
  // this Runnable would forcibly halt the process
  public class ForceExitTask implements Runnable {
    private final CountDownLatch exited;
    private final Duration timeout;

    public ForceExitTask(CountDownLatch exited, Duration timeout) {
      this.exited = exited;
      this.timeout = timeout;
    }

    @Override
    public void run() {
      LOG.info(String.format("Waiting for process exit in %s", timeout));
      boolean ret = false;
      try {
        ret = exited.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        LOG.log(Level.SEVERE, "ForceExitTask is interrupted:", e);
      }

      if (!ret) {
        LOG.severe("Failed to complete in time. Forcing exiting the process...\n");
        Runtime.getRuntime().halt(1);
      }
    }
  }
}
