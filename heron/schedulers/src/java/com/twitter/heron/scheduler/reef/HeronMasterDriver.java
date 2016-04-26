// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License

package com.twitter.heron.scheduler.reef;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.context.ContextConfiguration;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.evaluator.EvaluatorRequest;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.evaluator.FailedEvaluator;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;

import com.twitter.heron.scheduler.SchedulerMain;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.Cluster;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.Environ;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.HeronCorePackageName;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.HttpPort;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.Role;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.TopologyJar;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.TopologyName;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.TopologyPackageName;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.common.PackingPlan.ContainerPlan;
import com.twitter.heron.spi.common.PackingPlan.Resource;
import com.twitter.heron.spi.utils.TopologyUtils;

/**
 * {@link HeronMasterDriver} serves Heron Scheduler by managing containers / processes for Heron
 * TMaster and workers using REEF framework. This includes making container request for topology,
 * providing bits to start Heron components and killing containers.
 */
@Unit
public class HeronMasterDriver {
  private static final String TMASTER_CONTAINER_ID = "0";
  private static final int MB = 1024 * 1024;
  private static final int TM_MEM_SIZE_MB = 1024;
  private static final Logger LOG = Logger.getLogger(HeronMasterDriver.class.getName());

  // TODO(mfu):
  // TODO(mfu): 1. Have we handled the precise reconstruction of it after scheduler process dies unexpectedly and restarts?
  // TODO(mfu): 2. Need we make it a thread-safe Map, for instance, ConcurrentHashMap, since I see below it is also used not in synchronized block.
  private final Map<String, ActiveContext> contexts = new HashMap<>();
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
  // Currently yarn does not support mapping container requests to allocation (YARN-4879). As a result it is not
  // possible to concurrently start containers of different sizes. This lock ensures workers are started serially.
  // TODO(mfu): It may not be a good idea to start workers serially, since deployment of job can be super slow, in our experiences in Twitter.
  // TODO(mfu): An option is let the PackingAlgorithm returns always same size container, which in fact is what we do in Twitter.
  // TODO(mfu): Even trying to make works starting serially, better to use Semaphore.
  private final Object containerAllocationLock = new Object();
  private PackingPlan packing;
  // Container request submission and allocation takes places on different threads. This variable is used to share the
  // heron executor id for which the container request was submitted
  private String heronExecutorId;

  // This map will be needed to make container requests for a failed heron executor
  // TODO(mfu):
  // TODO(mfu): 1. Have we handled the precise reconstruction of it after scheduler process dies unexpectedly and restarts?
  // TODO(mfu): 2. Need we make it a thread-safe Map, for instance, ConcurrentHashMap, since I see below it is also used not in synchronized block.
  private Map<String, String> reefContainerToHeronExecutorMap = new HashMap<>();

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
  void scheduleTopologyMaster() {
    // TODO This method should be invoked only once per topology. Need to add some guards against subsequent
    // invocations?
    LOG.log(Level.INFO, "Scheduling container for TM: {0}", topologyName);
    try {
      // TODO(mfu): It is designed that TMaster is co-located in the same host with Scheduler. Otherwise, handling of some cases, for instance, network partition can be hard.
      // TODO(mfu): you may leave it as is, but add an issue to trace it.
      requestContainerForExecutor(TMASTER_CONTAINER_ID, 1, TM_MEM_SIZE_MB);
    } catch (InterruptedException e) {
      // Deployment of topology fails if there is a error starting TMaster
      throw new RuntimeException("Error while waiting for topology master container allocation", e);
    }
  }

  /**
   * Container allocation is asynchronous. Request containers serially to ensure allocated resources
   * match the required resources
   */
  void scheduleHeronWorkers(PackingPlan packing) {
    this.packing = packing;
    for (Entry<String, ContainerPlan> entry : packing.containers.entrySet()) {
      Resource reqResource = entry.getValue().resource;

      int mem = getMemInMBForExecutor(reqResource);
      try {
        requestContainerForExecutor(entry.getKey(), getCpuForExecutor(reqResource), mem);
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Error while waiting for container allocation for workers; Continue container request for remaining workers", e);
        // TODO(mfu): So just log as WARNING without any actions on it?
      }
    }
  }

  private void requestContainerForExecutor(String executorId, int cpu, int mem) throws InterruptedException {
    LOG.log(Level.INFO, "Scheduling container for executor, id: {0}", executorId);
    synchronized (containerAllocationLock) {
      heronExecutorId = executorId;
      requestor.submit(EvaluatorRequest.newBuilder().setNumber(1).setMemory(mem).setNumberOfCores(cpu).build());
      containerAllocationLock.wait();
    }
    LOG.log(Level.INFO, "Container is allocated for executor, id: {0}", executorId);
  }

  public void killTopology() {
    LOG.log(Level.INFO, "Kill topology: {0}", topologyName);
    for (Entry<String, ActiveContext> entry : contexts.entrySet()) {
      LOG.log(Level.INFO, "Close context: {0}", entry.getKey());
      entry.getValue().close();
    }
  }

  private int getCpuForExecutor(Resource resource) {
    return resource.cpu.intValue();
  }

  private int getMemInMBForExecutor(Resource resource) {
    Long ram = resource.ram / MB;
    return ram.intValue();
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
        // initialize the scheduler with the options
        SchedulerMain schedulerMain = new SchedulerMain(cluster, role, env, topologyName, topologyJar, httpPort);
        schedulerMain.runScheduler();
      } catch (IOException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        throw new RuntimeException("Failed to launch Heron Scheduler", e);
      }
    }
  }

  /**
   * Initializes worker on the allocated container
   */
  class HeronExecutorContainerBuilder implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(AllocatedEvaluator evaluator) {
      String executorId;
      synchronized (containerAllocationLock) {
        // create a local copy of executorId so that lock for next allocation can be released
        executorId = heronExecutorId;
        // TODO(mfu): Even for this case, Semaphore is a better choice. Wait/Notify is error prone.
        containerAllocationLock.notifyAll();
      }

      LOG.log(Level.INFO, "Start {0} for heron executor, id: {1}", new Object[]{evaluator.getId(), executorId});
      Configuration context = ContextConfiguration.CONF.set(ContextConfiguration.IDENTIFIER, executorId).build();
      evaluator.submitContext(context);

      reefContainerToHeronExecutorMap.put(evaluator.getId(), executorId);
    }
  }

  /**
   * Initializes worker on the allocated container
   */
  class HeronExecutorContainerErrorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(FailedEvaluator evaluator) {
      synchronized (HeronMasterDriver.class) {
        String executorId = reefContainerToHeronExecutorMap.get(evaluator.getId());
        LOG.log(Level.WARNING, "Container:{0} executor:{1} failed", new Object[]{evaluator.getId(), executorId});
        if (executorId == null) {
          LOG.log(Level.SEVERE, "Unknown executorId for failed container: {0}, skip renew action", evaluator.getId());
          return;
        }

        // TODO verify if this thread can be used to submit a new request
        // TODO(mfu): I don't think this implementation works:
        // TODO(mfu): 1. requestContainerForExecutor(..) is a blocking call
        // TODO(mfu): 2. invoking it here blocks the ReefEventHandler thread so this thread is no longer able to handle more events
        // TODO(mfu): 3. the lock.wait() will be no way to notify; eventually the whole scheduling will halt here
        try {
          if (executorId.equals(TMASTER_CONTAINER_ID)) {
            requestContainerForExecutor(TMASTER_CONTAINER_ID, 1, TM_MEM_SIZE_MB);
          } else {
            if (packing.containers.get(executorId) == null) {
              LOG.log(Level.SEVERE, "Missing container {0} in packing, skipping container request", executorId);
              return;
            }
            Resource reqResource = packing.containers.get(executorId).resource;
            requestContainerForExecutor(executorId, getCpuForExecutor(reqResource), getMemInMBForExecutor(reqResource));
          }
        } catch (InterruptedException e) {
          LOG.log(Level.WARNING, "Error waiting for container allocation for failed executor; Assuming request was submitted and continuing" + executorId, e);
        }
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
      String id = context.getId();
      contexts.put(id, context);
      LOG.log(Level.INFO, "Submitting evaluator task for id: {0}", id);

      // topologyName and other configurations are required by Heron Executor and Task to load
      // configuration files. Using REEF configuration model is better than depending on external
      // persistence.
      final Configuration taskConf = HeronTaskConfiguration.CONF
          .set(TaskConfiguration.TASK, HeronExecutorTask.class)
          .set(TaskConfiguration.IDENTIFIER, id)
          .set(HeronTaskConfiguration.TOPOLOGY_NAME, topologyName)
          .set(HeronTaskConfiguration.TOPOLOGY_JAR, topologyJar)
          .set(HeronTaskConfiguration.TOPOLOGY_PACKAGE_NAME, topologyPackageName)
          .set(HeronTaskConfiguration.HERON_CORE_PACKAGE_NAME, heronCorePackageName)
          .set(HeronTaskConfiguration.ROLE, role)
          .set(HeronTaskConfiguration.ENV, env)
          .set(HeronTaskConfiguration.CLUSTER, cluster)
          .set(HeronTaskConfiguration.PACKED_PLAN, TopologyUtils.packingToString(packing))
          .set(HeronTaskConfiguration.CONTAINER_ID, id)
          .build();
      context.submitTask(taskConf);
    }
  }
}
