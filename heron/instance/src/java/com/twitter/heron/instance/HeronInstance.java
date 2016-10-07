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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.Constants;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.utils.logging.ErrorReportLoggingHandler;
import com.twitter.heron.common.utils.logging.LoggingHelper;
import com.twitter.heron.common.utils.misc.ThreadNames;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.proto.system.Metrics;
import com.twitter.heron.proto.system.PhysicalPlans;

/**
 * HeronInstance is just an entry with resource initiation.
 */

public class HeronInstance {
  private static final Logger LOG = Logger.getLogger(HeronInstance.class.getName());

  private static final int NUM_THREADS = 2;

  private final NIOLooper gatewayLooper;
  private final SlaveLooper slaveLooper;

  // Only one outStreamQueue, which is responsible for both control tuples and data tuples
  private final Communicator<HeronTuples.HeronTupleSet> outStreamQueue;

  // This blocking queue is used to buffer tuples read from socket and ready to be used by instance
  // For spout, it will buffer Control tuple, while for bolt, it will buffer data tuple.
  private final Communicator<HeronTuples.HeronTupleSet> inStreamQueue;

  // This queue is used to pass Control Message from Gateway to Slave
  // TODO:- currently it would just pass the PhysicalPlanHelper
  // TODO:- we might handle more types of ControlMessage in future
  private final Communicator<InstanceControlMsg> inControlQueue;

  // This blocking queue is used to buffer the metrics info ready to send out to metrics manager
  // The 0th queue will be used to buffer the system metrics collected by Gateway
  // The 1th queue will be used to buffer the instance metrics collected by Instance
  private final List<Communicator<Metrics.MetricPublisherPublishMessage>> outMetricsQueues;

  private final Gateway gateway;
  private final Slave slave;

  private final ExecutorService threadsPool;

  private final SystemConfig systemConfig;

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
    slaveLooper = new SlaveLooper();

    // Add the task on exit
    gatewayLooper.addTasksOnExit(new GatewayExitTask());
    slaveLooper.addTasksOnExit(new SlaveExitTask());

    // For stream
    inStreamQueue = new Communicator<HeronTuples.HeronTupleSet>(gatewayLooper, slaveLooper);
    outStreamQueue = new Communicator<HeronTuples.HeronTupleSet>(slaveLooper, gatewayLooper);
    inControlQueue = new Communicator<InstanceControlMsg>(gatewayLooper, slaveLooper);

    // Now for metrics
    // No need in queues for metrics
    outMetricsQueues = new ArrayList<Communicator<Metrics.MetricPublisherPublishMessage>>(2);

    Communicator<Metrics.MetricPublisherPublishMessage> gatewayMetricsOut =
        new Communicator<Metrics.MetricPublisherPublishMessage>(gatewayLooper, gatewayLooper);
    gatewayMetricsOut.init(systemConfig.getInstanceInternalMetricsWriteQueueCapacity(),
        systemConfig.getInstanceTuningExpectedMetricsWriteQueueSize(),
        systemConfig.getInstanceTuningCurrentSampleWeight());

    Communicator<Metrics.MetricPublisherPublishMessage> slaveMetricsOut =
        new Communicator<Metrics.MetricPublisherPublishMessage>(slaveLooper, gatewayLooper);
    slaveMetricsOut.init(systemConfig.getInstanceInternalMetricsWriteQueueCapacity(),
        systemConfig.getInstanceTuningExpectedMetricsWriteQueueSize(),
        systemConfig.getInstanceTuningCurrentSampleWeight());

    outMetricsQueues.add(gatewayMetricsOut);
    outMetricsQueues.add(slaveMetricsOut);

    // We will new these two Runnable
    this.gateway =
        new Gateway(topologyName, topologyId, instance, streamPort, metricsPort,
            gatewayLooper, inStreamQueue, outStreamQueue, inControlQueue, outMetricsQueues);
    this.slave = new Slave(slaveLooper, inStreamQueue, outStreamQueue,
        inControlQueue, slaveMetricsOut);

    // New the ThreadPool and register it inside the SingletonRegistry
    threadsPool = Executors.newFixedThreadPool(NUM_THREADS);
  }

  public static void main(String[] args) throws IOException {
    if (args.length < 10) {
      throw new RuntimeException(
          "Invalid arguments; Usage is java com.twitter.heron.instance.HeronInstance "
              + "<topology_name> <topology_id> <instance_id> <component_name> <task_id> "
              + "<component_index> <stmgr_id> <stmgr_port> <metricsmgr_port> "
              + "<heron_internals_config_filename>");
    }

    String topologyName = args[0];
    String topologyId = args[1];
    String instanceId = args[2];
    String componentName = args[3];
    int taskId = Integer.parseInt(args[4]);
    int componentIndex = Integer.parseInt(args[5]);
    String streamId = args[6];
    int streamPort = Integer.parseInt(args[7]);
    int metricsPort = Integer.parseInt(args[8]);
    SystemConfig systemConfig = new SystemConfig(args[9], true);

    // Add the SystemConfig into SingletonRegistry
    SingletonRegistry.INSTANCE.registerSingleton(SystemConfig.HERON_SYSTEM_CONFIG, systemConfig);

    // Create the protobuf Instance
    PhysicalPlans.InstanceInfo instanceInfo = PhysicalPlans.InstanceInfo.newBuilder().
        setTaskId(taskId).setComponentIndex(componentIndex).setComponentName(componentName).build();

    PhysicalPlans.Instance instance = PhysicalPlans.Instance.newBuilder().
        setInstanceId(instanceId).setStmgrId(streamId).setInfo(instanceInfo).build();

    // Init the logging setting and redirect the stdout and stderr to logging
    // For now we just set the logging level as INFO; later we may accept an argument to set it.
    Level loggingLevel = Level.INFO;
    String loggingDir = systemConfig.getHeronLoggingDirectory();

    // Log to file and TMaster
    LoggingHelper.loggerInit(loggingLevel, true);
    LoggingHelper.addLoggingHandler(
        LoggingHelper.getFileHandler(instanceId, loggingDir, true,
            systemConfig.getHeronLoggingMaximumSizeMb() * Constants.MB_TO_BYTES,
            systemConfig.getHeronLoggingMaximumFiles()));
    LoggingHelper.addLoggingHandler(new ErrorReportLoggingHandler());

    LOG.info("\nStarting instance " + instanceId + " for topology " + topologyName
        + " and topologyId " + topologyId + " for component " + componentName
        + " with taskId " + taskId + " and componentIndex " + componentIndex
        + " and stmgrId " + streamId + " and stmgrPort " + streamPort
        + " and metricsManagerPort " + metricsPort);

    LOG.info("System Config: " + systemConfig);

    HeronInstance heronInstance =
        new HeronInstance(topologyName, topologyId, instance, streamPort, metricsPort);
    heronInstance.start();
  }

  public void start() {
    // Add exception handler for any uncaught exception here.
    Thread.setDefaultUncaughtExceptionHandler(new DefaultExceptionHandler());

    // Get the Thread Pool and run it
    threadsPool.execute(gateway);
    threadsPool.execute(slave);
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
      LOG.log(Level.SEVERE,
          String.format("Exception caught in thread: %s with id: %d",
              thread.getName(), thread.getId()), exception);

      // CountDownLatch to notify ForceExitTask whether exit is done
      final CountDownLatch exited = new CountDownLatch(1);
      final ExecutorService exitExecutor = Executors.newSingleThreadExecutor();
      exitExecutor.execute(
          new ForceExitTask(exited, systemConfig.getInstanceForceExitTimeoutMs()));

      // Clean up
      if (thread.getName().equals(ThreadNames.THREAD_SLAVE_NAME)) {
        // Run the SlaveExitTask here since the thread throw exceptions
        // and this Task would never be invoked on exit in future
        new SlaveExitTask().run();

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

  // The Task to execute on Slave thread's exit
  public class SlaveExitTask implements Runnable {

    @Override
    public void run() {
      SysUtils.closeIgnoringExceptions(slave);
    }
  }

  // The task is to guarantee this process would exit in certain timeout
  // It would receive a CountDownLatch as a notifier and a timeout value in long.
  // If the CountDownLatch does not be 0 in the timeout,
  // this Runnable would forcibly halt the process
  public class ForceExitTask implements Runnable {
    private final CountDownLatch exited;
    private final long timeoutInMs;

    public ForceExitTask(CountDownLatch exited, long timeoutInMs) {
      this.exited = exited;
      this.timeoutInMs = timeoutInMs;
    }

    @Override
    public void run() {
      LOG.info(String.format("Waiting for process exit in %d ms...", timeoutInMs));
      boolean ret = false;
      try {
        ret = exited.await(timeoutInMs, TimeUnit.MILLISECONDS);
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
