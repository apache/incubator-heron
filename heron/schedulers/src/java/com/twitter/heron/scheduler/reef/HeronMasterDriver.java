package com.twitter.heron.scheduler.reef;

import com.twitter.heron.scheduler.SchedulerMain;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.*;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.common.PackingPlan.ContainerPlan;
import com.twitter.heron.spi.common.PackingPlan.Resource;
import com.twitter.heron.spi.common.ShellUtils;
import com.twitter.heron.spi.utils.TopologyUtils;
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

import javax.inject.Inject;
import java.io.File;
import java.io.IOException;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link HeronMasterDriver} serves Heron Scheduler by managing containers / processes for Heron TMaster and workers
 * using REEF framework. This includes making container request for topology, providing bits to start Heron components
 * and killing containers.
 */
@Unit
public class HeronMasterDriver {
  public static final String TMASTER_CONTAINER_ID = "0";
  private static final int MB = 1024 * 1024;
  private static final int TM_MEM_SIZE_MB = 1024;
  private static final Logger LOG = Logger.getLogger(HeronMasterDriver.class.getName());

  /**
   * Heron-REEF scheduler is initialized by {@link SchedulerMain}. This instance of {@link HeronMasterDriver} is needed
   * by the scheduler to use the containers.
   */
  private static HeronMasterDriver instance;

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

  private PackingPlan packing;

  // Currently yarn does not support mapping container requests to allocation (YARN-4879). As a result it is not
  // possible to concurrently start containers of different sizes. This lock ensures workers are started serially.
  private final Object containerAllocationLock = new Object();

  // Container request submission and allocation takes places on different threads. This variable is used to share the
  // heron executor id for which the container request was submitted
  private String heronExecutorId;

  // This map will be needed to make container requests for a failed heron executor
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
    reefFileNames = fileNames;

    // Heron related initialization
    localHeronConfDir = ".";
    this.cluster = cluster;
    this.role = role;
    this.topologyName = topologyName;
    this.topologyPackageName = topologyPackageName;
    this.heronCorePackageName = heronCorePackageName;
    this.env = env;
    this.topologyJar = topologyJar;
    this.httpPort = httpPort;

    // Driver singleton
    instance = this;
  }

  synchronized public static HeronMasterDriver getInstance() {
    return instance;
  }

  /**
   * Requests container for TMaster as container/executor id 0.
   */
  void scheduleTopologyMaster() {
    // TODO This method should be invoked only once per topology. Need to add some guards against subsequent
    // invocations?
    LOG.log(Level.INFO, "Scheduling container for TM: {0}", topologyName);
    try {
      requestContainerForExecutor(TMASTER_CONTAINER_ID, 1, TM_MEM_SIZE_MB);
    } catch (InterruptedException e) {
      // Deployment of topology fails if there is a error starting TMaster
      LOG.log(Level.WARNING, "Error while waiting for topology master container allocation", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Container allocation is asynchronous. Request containers serially to ensure allocated resources match the required
   * resources
   */
  void scheduleHeronWorkers(PackingPlan packing) {
    this.packing = packing;
    for (Entry<String, ContainerPlan> entry : packing.containers.entrySet()) {
      Resource reqResource = entry.getValue().resource;

//      TODO fix mem computation int mem = getMemInMB(reqResource.ram);
      int mem = 512;
      try {
        requestContainerForExecutor(entry.getKey(), getCpuForExecutor(reqResource), mem);
      } catch (InterruptedException e) {
        LOG.log(Level.WARNING, "Error while waiting for container allocation for workers", e);
        LOG.log(Level.WARNING, "Continue container request for remaining workers", e);
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
   * {@link HeronSchedulerLauncher} is the first class initialized on the server by REEF. This is responsible for
   * unpacking binaries and launching Heron Scheduler.
   */
  class HeronSchedulerLauncher implements EventHandler<StartTime> {
    @Override
    public void onNext(StartTime value) {
      String globalFolder = reefFileNames.getGlobalFolder().getPath();

      extractPackageInSandbox(globalFolder, topologyPackageName, localHeronConfDir);
      extractPackageInSandbox(globalFolder, heronCorePackageName, localHeronConfDir);

      launchSchedulerServer();
    }

    private void launchSchedulerServer() {
      try {
        // initialize the scheduler with the options
        SchedulerMain schedulerMain = new SchedulerMain(cluster, role, env, topologyName, topologyJar, httpPort);
        schedulerMain.runScheduler();
      } catch (IOException | ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        LOG.log(Level.SEVERE, "Failed to launch Heron Scheduler", e);
        throw new RuntimeException(e);
      }
    }

    private void extractPackageInSandbox(String srcFolder, String fileName, String dstDir) {
      String packagePath = Paths.get(srcFolder, fileName).toString();
      LOG.log(Level.INFO, "Extracting package: {0} at: {1}", new Object[]{packagePath, dstDir});
      boolean result = untarPackage(packagePath, dstDir);
      if (!result) {
        String msg = "Failed to extract package:" + packagePath + " at: " + dstDir;
        LOG.log(Level.SEVERE, msg);
        throw new RuntimeException(msg);
      }
    }

    /**
     * TODO this method from LocalLauncher could be moved to a utils class
     */
    protected boolean untarPackage(String packageName, String targetFolder) {
      String cmd = String.format("tar -xvf %s", packageName);

      int ret = ShellUtils.runSyncProcess(false,
              true,
              cmd,
              new StringBuilder(),
              new StringBuilder(),
              new File(targetFolder));

      return ret == 0 ? true : false;
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
          LOG.log(Level.WARNING, "Error waiting for container allocation for failed executor: " + executorId, e);
          LOG.log(Level.WARNING, "Assuming request was submitted and continuing", e);
        }
      }
    }
  }

  /**
   * Once the container starts, this class starts Heron's executor process. Heron executor is started as a task. This
   * task can be killed and the container can be reused.
   */
  public final class HeronExecutorLauncher implements EventHandler<ActiveContext> {
    @Override
    public void onNext(ActiveContext context) {
      String id = context.getId();
      contexts.put(id, context);
      LOG.log(Level.INFO, "Submitting evaluator task for id: {0}", id);

      final Configuration taskConf = HeronTaskConfiguration.CONF.set(TaskConfiguration.TASK, HeronExecutorTask.class)
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
