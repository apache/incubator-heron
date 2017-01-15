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

package com.twitter.heron.scheduler.yarn;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import com.twitter.heron.scheduler.SchedulerMain;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.Cluster;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.Environ;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.HeronCorePackageName;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.HttpPort;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.Role;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.TopologyJar;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.TopologyName;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.TopologyPackageName;
import com.twitter.heron.spi.packing.PackingPlan;
import com.twitter.heron.spi.packing.PackingPlan.ContainerPlan;
import com.twitter.heron.spi.packing.PackingPlan.Resource;

/**
 * {@link HeronMasterDriver} serves Heron's YARN Scheduler by managing containers / processes for
 * Heron TMaster and workers using REEF framework. This includes making container request for
 * topology, providing package needed to start Heron components and killing containers.
 */
@Unit
public class HeronMasterDriver {
  static final int TM_MEM_SIZE_MB = 1024;
  static final String TMASTER_CONTAINER_ID = "0";
  static final int MB = 1024 * 1024;
  private static final Logger LOG = Logger.getLogger(HeronMasterDriver.class.getName());
  private final String topologyPackageName;
  private final String heronCorePackageName;
  private final EvaluatorRequestor requestor;
  private final REEFFileNames reefFileNames;
  private final String localHeronConfDir;
  private final String cluster;
  private final String role;
  private final String topologyName;
  private final String env;
  private final String topologyJar;
  private final int httpPort;
  // TODO: https://github.com/twitter/heron/issues/949: implement Driver HA
  // Once topology is killed, no more activeContexts should be allowed. This could happen when
  // container allocation is happening and topology is killed concurrently.
  private Map<String, ActiveContext> activeContexts = new ConcurrentHashMap<>();
  // TODO: https://github.com/twitter/heron/issues/950, support homogeneous container allocation
  // Currently yarn does not support mapping container requests to allocation (YARN-4879). As a
  // result it is not possible to concurrently start containers of different sizes. This Executor
  // and Queue will ensure containers are started serially.
  private ExecutorService executor = Executors.newSingleThreadExecutor();
  private BlockingQueue<AllocatedEvaluator> allocatedContainerQ = new LinkedBlockingDeque<>();

  // This map will be needed to make container requests for a failed heron executor
  private Map<String, String> reefContainerToHeronExecutorMap = new ConcurrentHashMap<>();

  // On topology restarts all existing tasks need to be killed. This map maintains handlers for all
  // currently running tasks and mapped to the owner container.
  private Map<RunningTask, ActiveContext> runningTaskToContextMap = new ConcurrentHashMap<>();

  private PackingPlan packing;

  @Inject
  public HeronMasterDriver(EvaluatorRequestor requestor,
                           final REEFFileNames fileNames,
                           @Parameter(Cluster.class) String cluster,
                           @Parameter(Role.class) String role,
                           @Parameter(TopologyName.class) String topologyName,
                           @Parameter(Environ.class) String env,
                           @Parameter(TopologyJar.class) String topologyJar,
                           @Parameter(TopologyPackageName.class) String topologyPackageName,
                           @Parameter(HeronCorePackageName.class) String heronCorePackageName,
                           @Parameter(HttpPort.class) int httpPort) throws IOException {

    // REEF related initialization
    this.requestor = requestor;
    this.reefFileNames = fileNames;

    // Heron related initialization
    this.localHeronConfDir = ".";
    this.cluster = cluster;
    this.role = role;
    this.topologyName = topologyName;
    this.topologyPackageName = topologyPackageName;
    this.heronCorePackageName = heronCorePackageName;
    this.env = env;
    this.topologyJar = topologyJar;
    this.httpPort = httpPort;

    // This instance of Driver will be used for managing topology containers
    HeronMasterDriverProvider.setInstance(this);
  }

  /**
   * Requests container for TMaster as container/executor id 0.
   */
  void scheduleTMasterContainer() {
    LOG.log(Level.INFO, "Scheduling container for TM: {0}", topologyName);
    try {
      // TODO: https://github.com/twitter/heron/issues/951: colocate TMaster and Scheduler
      launchContainerForExecutor(TMASTER_CONTAINER_ID, 1, TM_MEM_SIZE_MB);
    } catch (InterruptedException e) {
      // Deployment of topology fails if there is a error starting TMaster
      throw new RuntimeException("Error while waiting for topology master container allocation", e);
    }
  }

  /**
   * Container allocation is asynchronous. Request containers serially to ensure allocated resources
   * match the required resources
   */
  void scheduleHeronWorkers(PackingPlan topologyPacking) {
    this.packing = topologyPacking;
    for (Entry<String, ContainerPlan> entry : topologyPacking.containers.entrySet()) {
      Resource reqResource = entry.getValue().resource;

      int mem = getMemInMBForExecutor(reqResource);
      try {
        launchContainerForExecutor(entry.getKey(), getCpuForExecutor(reqResource), mem);
      } catch (InterruptedException e) {
        // Fail deployment of topology if there is a error starting any worker
        LOG.log(Level.SEVERE, "Container allocation for id:{0} failed. Attempting to close "
            + "currently allocated resources for {1}", new Object[]{entry.getKey(), topologyName});
        killTopology();
        throw new RuntimeException("Failed to allocate container for workers", e);
      }
    }
  }

  public void killTopology() {
    LOG.log(Level.INFO, "Kill topology: {0}", topologyName);
    Map<String, ActiveContext> contexts = clearActiveContexts();

    for (Entry<String, ActiveContext> entry : contexts.entrySet()) {
      LOG.log(Level.INFO, "Close context: {0}", entry.getKey());
      entry.getValue().close();
    }
  }

  /**
   * REEF will retain all YARN containers. Currently running heron-executors/REEF-tasks
   * will be killed and new REEF-tasks representing previously running heron-executor will be
   * started.
   */
  public void restartTopology() {
    restartContainer(null);
  }

  public void restartContainer(String id) {
    if (id == null) {
      LOG.log(Level.INFO, "Restarting all tasks of topology: {0}", topologyName);
    } else {
      LOG.log(Level.INFO, "Find & restart task for id={0} in: {1}", new Object[]{id, topologyName});
    }

    for (RunningTask runningTask : runningTaskToContextMap.keySet()) {
      if (id != null && !runningTask.getId().equals(id)) {
        continue;
      }
      ActiveContext context = runningTaskToContextMap.remove(runningTask);
      LOG.log(Level.INFO, "Killing task: {0}", runningTask.getId());
      runningTask.close();
      submitHeronExecutorTask(context);
    }
  }

  private void launchContainerForExecutor(final String executorId, final int cpu, final int mem)
      throws InterruptedException {
    executor.submit(new Callable<AllocatedEvaluator>() {
      @Override
      public AllocatedEvaluator call() throws Exception {
        AllocatedEvaluator evaluator = allocateContainer(executorId, cpu, mem);

        Configuration context = createContextConfig(executorId);

        LOG.log(Level.INFO,
            "Activating container {0} for heron executor, id: {1}",
            new Object[]{evaluator.getId(), executorId});
        evaluator.submitContext(context);
        reefContainerToHeronExecutorMap.put(evaluator.getId(), executorId);
        return evaluator;
      }
    });
  }

  AllocatedEvaluator allocateContainer(String id, int cpu, int mem) throws InterruptedException {
    EvaluatorRequest evaluatorRequest = createEvaluatorRequest(cpu, mem);

    LOG.log(Level.INFO, "Requesting container for executor, id: {0}, mem: {1}, cpu: {2}",
        new Object[]{id, mem, cpu});
    requestor.submit(evaluatorRequest);

    AllocatedEvaluator evaluator = allocatedContainerQ.take();
    LOG.log(Level.INFO,
        "Container {0} is allocated for executor, id: {1}",
        new Object[]{evaluator.getId(), id});
    return evaluator;
  }

  Configuration createContextConfig(String executorId) {
    return ContextConfiguration.CONF
        .set(ContextConfiguration.IDENTIFIER, executorId)
        .build();
  }

  EvaluatorRequest createEvaluatorRequest(int cpu, int mem) {
    return EvaluatorRequest
        .newBuilder()
        .setNumber(1)
        .setMemory(mem)
        .setNumberOfCores(cpu)
        .build();
  }

  private int getCpuForExecutor(Resource resource) {
    return (int) Math.ceil(resource.cpu);
  }

  private int getMemInMBForExecutor(Resource resource) {
    Long ram = resource.ram / MB;
    return ram.intValue();
  }

  String getPackingAsString() {
    return packing.getInstanceDistribution();
  }

  String getComponentRamMap() {
    return packing.getComponentRamDistribution();
  }

  boolean addActiveContext(ActiveContext context) {
    if (activeContexts == null) {
      LOG.log(Level.WARNING, "Topology has been killed, new context ignored and closed.");
      context.close();
      return false;
    }

    String key = context.getId();
    ActiveContext orphanedContext = activeContexts.get(key);
    if (orphanedContext != null) {
      LOG.log(Level.WARNING, "Found orphaned context for id: {0}, will close it", key);
      orphanedContext.close();
    }

    activeContexts.put(key, context);
    return true;
  }

  Map<String, ActiveContext> clearActiveContexts() {
    Map<String, ActiveContext> result = activeContexts;
    activeContexts = null;
    return result;
  }

  void submitHeronExecutorTask(ActiveContext context) {
    String id = context.getId();
    LOG.log(Level.INFO, "Submitting evaluator task for id: {0}", id);

    // topologyName and other configurations are required by Heron Executor and Task to load
    // configuration files. Using REEF configuration model is better than depending on external
    // persistence.
    final Configuration taskConf = HeronTaskConfiguration.CONF
        .set(TaskConfiguration.TASK, HeronExecutorTask.class)
        .set(TaskConfiguration.ON_CLOSE, HeronExecutorTask.HeronExecutorTaskTerminator.class)
        .set(TaskConfiguration.IDENTIFIER, id)
        .set(HeronTaskConfiguration.TOPOLOGY_NAME, topologyName)
        .set(HeronTaskConfiguration.TOPOLOGY_JAR, topologyJar)
        .set(HeronTaskConfiguration.TOPOLOGY_PACKAGE_NAME, topologyPackageName)
        .set(HeronTaskConfiguration.HERON_CORE_PACKAGE_NAME, heronCorePackageName)
        .set(HeronTaskConfiguration.ROLE, role)
        .set(HeronTaskConfiguration.ENV, env)
        .set(HeronTaskConfiguration.CLUSTER, cluster)
        .set(HeronTaskConfiguration.PACKED_PLAN, getPackingAsString())
        .set(HeronTaskConfiguration.COMPONENT_RAM_MAP, getComponentRamMap())
        .set(HeronTaskConfiguration.CONTAINER_ID, id)
        .build();
    context.submitTask(taskConf);
  }

  /**
   * {@link HeronSchedulerLauncher} is the first class initialized on the server by REEF. This is
   * responsible for unpacking binaries and launching Heron Scheduler.
   */
  class HeronSchedulerLauncher implements EventHandler<StartTime> {
    @Override
    public void onNext(StartTime value) {
      String globalFolder = reefFileNames.getGlobalFolder().getPath();

      HeronReefUtils.extractPackageInSandbox(globalFolder, topologyPackageName, localHeronConfDir);
      HeronReefUtils.extractPackageInSandbox(globalFolder, heronCorePackageName, localHeronConfDir);

      launchScheduler();
    }

    private void launchScheduler() {
      try {
        LOG.log(Level.INFO, "Launching Heron scheduler");
        SchedulerMain schedulerMain = SchedulerMain.createInstance(cluster,
            role,
            env,
            topologyJar,
            topologyName,
            httpPort);
        schedulerMain.runScheduler();
      } catch (IOException e) {
        throw new RuntimeException("Failed to launch Heron Scheduler", e);
      }
    }
  }

  /**
   * Initializes worker on the allocated container
   */
  class HeronContainerAllocationHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(AllocatedEvaluator evaluator) {
      LOG.log(Level.INFO, "New container received, id: {0}", evaluator.getId());
      try {
        allocatedContainerQ.put(evaluator);
        LOG.log(Level.INFO, "{0} containers waiting for activation", allocatedContainerQ.size());
      } catch (InterruptedException e) {
        // Fail deployment of topology if there is a error starting any worker
        LOG.log(Level.SEVERE, "Container allocation for id:{0} failed. Trying to close currently"
            + " allocated resources for {1}", new Object[]{evaluator.getId(), topologyName});
        killTopology();
        throw new RuntimeException("Failed to allocate container for workers", e);
      }
    }
  }

  /**
   * Initializes worker on the allocated container
   */
  class HeronExecutorContainerErrorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(FailedEvaluator evaluator) {
      String executorId = reefContainerToHeronExecutorMap.get(evaluator.getId());
      LOG.log(Level.WARNING,
          "Container:{0} executor:{1} failed",
          new Object[]{evaluator.getId(), executorId});
      if (executorId == null) {
        LOG.log(Level.SEVERE,
            "Unknown executorId for failed container: {0}, skip renew action",
            evaluator.getId());
        return;
      }

      try {
        if (executorId.equals(TMASTER_CONTAINER_ID)) {
          launchContainerForExecutor(TMASTER_CONTAINER_ID, 1, TM_MEM_SIZE_MB);
        } else {
          if (packing.containers.get(executorId) == null) {
            LOG.log(Level.SEVERE,
                "Missing container {0} in packing, skipping container request",
                executorId);
            return;
          }
          Resource reqResource = packing.containers.get(executorId).resource;
          launchContainerForExecutor(executorId,
              getCpuForExecutor(reqResource),
              getMemInMBForExecutor(reqResource));
        }
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Error waiting for container allocation for failed executor; "
            + "Assuming request was submitted and continuing" + executorId, e);
      }
    }
  }

  /**
   * Once the container starts, this class starts Heron's executor process. Heron executor is
   * started as a task. This task can be killed and the container can be reused.
   */
  public final class HeronExecutorLauncher implements EventHandler<ActiveContext> {
    @Override
    public void onNext(ActiveContext context) {
      boolean result = addActiveContext(context);
      if (!result) {
        return;
      }

      submitHeronExecutorTask(context);
    }
  }

  /**
   * This class manages active tasks. The task handlers provided by REEF will be memorized to be
   * used later for operations like topology restart. Restarting a task does not require new
   * container request.
   */
  public final class HeronRunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(RunningTask runningTask) {
      LOG.log(Level.INFO, "Task, id:{0}, has started. Count of running tasks: {1}",
          new Object[]{runningTask.getId(), runningTaskToContextMap.size() + 1});
      runningTaskToContextMap.put(runningTask, runningTask.getActiveContext());
    }
  }

  /**
   * Tasks will be closed when on a restart request from user. The intent is to retain the
   * container and just restart the executor process. Hence do-nothing when a task completes.
   */
  public final class HeronCompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(CompletedTask completedTask) {
      LOG.log(Level.INFO, "Task/executor completed, id:{0}. Count of running tasks: {1}",
          new Object[]{completedTask.getId(), runningTaskToContextMap.size() + 1});
    }
  }
}
