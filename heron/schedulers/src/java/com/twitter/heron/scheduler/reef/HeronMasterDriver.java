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
import java.util.concurrent.CountDownLatch;
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
  private static final int TM_MEM_SIZE_MB = 512;
  private static final Logger LOG = Logger.getLogger(HeronMasterDriver.class.getName());

  /**
   * Heron-REEF scheduler is initialized by {@link SchedulerMain}. This instance of {@link HeronMasterDriver} is needed
   * by the scheduler to use the containers.
   */
  private static HeronMasterDriver instance;

  private final Map<String, ActiveContext> contexts = new HashMap<>();
  private EvaluatorRequestor requestor;
  private REEFFileNames reefFileNames;
  private String localHeronConfDir;
  private PackingPlan packing;
  private String cluster;
  private String role;
  private String topologyName;
  private String env;
  private String topologyJar;
  private int httpPort;

  // TM must start before workers. This lock is used to coordinate this.
  private CountDownLatch topologyMasterActive = new CountDownLatch(1);

  // Currently yarn does not support mapping container requests to allocation (YARN-4879). As a result it is not
  // possible to concurrently start containers of different sizes. This lock ensures workers are started serially.
  private CountDownLatch workerActive;
  private String workerId;
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
                           @Parameter(HttpPort.class) int httpPort) throws IOException {

    // REEF related initialization
    this.requestor = requestor;
    reefFileNames = fileNames;

    // Heron related initialization
    localHeronConfDir = ".";
    this.cluster = cluster;
    this.role = role;
    this.topologyName = topologyName;
    this.env = env;
    this.topologyJar = topologyJar;
    this.httpPort = httpPort;

    // Driver singleton
    instance = this;
  }

  synchronized public static HeronMasterDriver getInstance() {
    return instance;
  }

  public void configureTopology(PackingPlan packing) {
    this.packing = packing;
  }

  public void scheduleTopologyMaster() {
    LOG.info("Scheduling container for TM: " + topologyName);
    requestor.submit(EvaluatorRequest.newBuilder().setMemory(TM_MEM_SIZE_MB).build());
    try {
      LOG.info("Waiting for TM container to be allocated: " + topologyName);
      topologyMasterActive.await();
      LOG.info("TM container is allocated: " + topologyName);
    } catch (InterruptedException e) {
      LOG.log(Level.WARNING, "Error while waiting for topology master container allocation", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Container allocation is asynchronous. Request containers serially to ensure allocated resources match the required
   * resources
   */
  public void scheduleWorkers() {
    for (Entry<String, ContainerPlan> entry : packing.containers.entrySet()) {
      workerId = entry.getKey();
      Resource reqResource = entry.getValue().resource;

      scheduleWorker(reqResource);
    }
  }

  private void scheduleWorker(Resource reqResource) {
    LOG.info("Scheduling container for worker: " + workerId);

//      TODO fix mem computation int mem = getMemInMB(reqResource.ram);
    int mem = 512;
    int cpu = Double.valueOf(reqResource.cpu).intValue();

    workerActive = new CountDownLatch(1);
    requestor.submit(EvaluatorRequest.newBuilder().setNumber(1).setMemory(mem).setNumberOfCores(cpu).build());

    try {
      LOG.info("Waiting for worker container to be allocated: " + workerId);
      workerActive.await();
      LOG.info("Worker container is allocated: " + workerId);
    } catch (InterruptedException e) {
      LOG.log(Level.WARNING, "Error while waiting for worker container allocation", e);
    }
  }

  /**
   * Launches executor for TMaster OR worker on the given REEF evaluator
   */
  private void launchHeronExecutor(AllocatedEvaluator evaluator, CountDownLatch latch, String containerId) {
    latch.countDown();
    reefContainerToHeronExecutorMap.put(evaluator.getId(), workerId);
    LOG.log(Level.INFO, "Launching evaluator {0} for executor ID: {1}", new Object[]{evaluator.getId(), containerId});

    Configuration contextConfig = ContextConfiguration.CONF.set(ContextConfiguration.IDENTIFIER, containerId).build();
    evaluator.submitContext(contextConfig);
  }

  public void killTopology() {
    LOG.info("Kill topology: " + topologyName);
    for (Entry<String, ActiveContext> entry : contexts.entrySet()) {
      LOG.info("Close context:" + entry.getKey());
      entry.getValue().close();
    }
  }

  private int getMemInMB(Long ram) {
    ram = ram / MB;
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

      // TODO pass topology name and core from config
      extractTopologyPkgInSandbox(globalFolder, localHeronConfDir);
      extractCorePkgInSandbox(globalFolder, localHeronConfDir);

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

    private void extractTopologyPkgInSandbox(String srcFolder, String dstDir) {
      String topologyPackage = Paths.get(srcFolder, "topology.tar.gz").toString();
      LOG.log(Level.INFO, "Extracting topology : {0} at: {1}", new Object[]{topologyPackage, dstDir});
      boolean result = untarPackage(topologyPackage, dstDir);
      if (!result) {
        LOG.log(Level.INFO, "Failed to extract topology package");
        throw new RuntimeException("Failed to extract topology package");
      }
    }

    private void extractCorePkgInSandbox(String srcFolder, String dstDir) {
      String corePackage = Paths.get(srcFolder, "heron-core.tar.gz").toString();
      LOG.log(Level.INFO, "Extracting core: {0} at: {1}", new Object[]{corePackage, dstDir});
      boolean result = untarPackage(corePackage, dstDir);
      if (!result) {
        LOG.log(Level.INFO, "Failed to extract core package");
        throw new RuntimeException("Failed to extract core package");
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
  class HeronWorkerBuilder implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(AllocatedEvaluator evaluator) {
      synchronized (HeronMasterDriver.class) {
        if (topologyMasterActive.getCount() == 1) {
          launchHeronExecutor(evaluator, topologyMasterActive, TMASTER_CONTAINER_ID);
        } else {
          launchHeronExecutor(evaluator, workerActive, workerId);
        }
      }
    }
  }

  /**
   * Initializes worker on the allocated container
   */
  class HeronWorkerErrorHandler implements EventHandler<FailedEvaluator> {
    @Override
    public void onNext(FailedEvaluator evaluator) {
      synchronized (HeronMasterDriver.class) {
        String heronExecutorId = reefContainerToHeronExecutorMap.get(evaluator.getId());
        LOG.log(Level.WARNING, "Container:{0} executor:{1} failed", new Object[]{evaluator.getId(), heronExecutorId});
        if (heronExecutorId.equals(TMASTER_CONTAINER_ID)) {
          // TODO verify if this thread can be used to submit a new request
          scheduleTopologyMaster();
        } else {
          workerId = heronExecutorId;
          Resource reqResource = packing.containers.get(workerId).resource;
          scheduleWorker(reqResource);
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
      LOG.info("Submitting evaluator task for id:" + id);

      final Configuration taskConf = HeronTaskConfiguration.CONF.set(TaskConfiguration.TASK, HeronExecutorTask.class)
              .set(TaskConfiguration.IDENTIFIER, id)
              .set(HeronTaskConfiguration.TOPOLOGY_NAME, topologyName)
              .set(HeronTaskConfiguration.TOPOLOGY_JAR, topologyJar)
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
