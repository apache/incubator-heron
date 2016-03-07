package com.twitter.heron.scheduler.mesos;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.scheduler.api.Constants;
import com.twitter.heron.scheduler.api.IScheduler;
import com.twitter.heron.scheduler.api.PackingPlan;
import com.twitter.heron.scheduler.api.SchedulerStateManagerAdaptor;
import com.twitter.heron.scheduler.api.context.LaunchContext;
import com.twitter.heron.scheduler.mesos.framework.config.FrameworkConfiguration;
import com.twitter.heron.scheduler.mesos.framework.driver.MesosDriverFactory;
import com.twitter.heron.scheduler.mesos.framework.driver.MesosJobFramework;
import com.twitter.heron.scheduler.mesos.framework.driver.MesosTaskBuilder;
import com.twitter.heron.scheduler.mesos.framework.jobs.BaseJob;
import com.twitter.heron.scheduler.mesos.framework.jobs.JobScheduler;
import com.twitter.heron.scheduler.mesos.framework.state.PersistenceStore;
import com.twitter.heron.scheduler.mesos.framework.state.ZkPersistenceStore;
import com.twitter.heron.scheduler.util.NetworkUtility;
import com.twitter.heron.scheduler.util.ShellUtility;

public class MesosScheduler implements IScheduler {
  private static final Logger LOG = Logger.getLogger(MesosScheduler.class.getName());

  private static final long MAX_WAIT_TIMEOUT_MS = 30 * 1000;

  private LaunchContext context;
  private AtomicBoolean tmasterRestart;
  private Thread tmasterRunThread;
  private Process tmasterProcess;

  private TopologyAPI.Topology topology;
  private String topologyName;
  private SchedulerStateManagerAdaptor stateManager;

  private JobScheduler jobScheduler;
  private volatile CountDownLatch startLatch;

  private String executorCmdTemplate = "";

  private final Map<Integer, BaseJob> executorShardToJob = new ConcurrentHashMap<>();

  private PersistenceStore persistenceStore;

  private MesosJobFramework mesosJobFramework;

  @Override
  public void initialize(LaunchContext context) {
    LOG.info("Initializing new mesos topology scheduler");
    this.tmasterRestart = new AtomicBoolean(true);
    this.context = context;
    this.stateManager = context.getStateManagerAdaptor();

    this.topology = context.getTopology();
    this.topologyName = context.getTopologyName();

    // Start the jobScheduler
    this.jobScheduler = getJobScheduler();
    this.jobScheduler.start();

    // Start tmaster.
    createTmasterRunScript();
    tmasterRunThread = new Thread(new Runnable() {
      @Override
      public void run() {
        runTmaster();
      }
    });
    tmasterRunThread.setDaemon(true);
    tmasterRunThread.start();

  }

  private void startRegularContainers(PackingPlan packing) {
    LOG.info("We are to start the new mesos job scheduler now");
    this.topology = context.getTopology();

    int shardId = 0;
    this.executorCmdTemplate = getExecutorCmdTemplate();

    for (PackingPlan.ContainerPlan container : packing.containers.values()) {
      shardId++;
      String executorCommand = String.format(executorCmdTemplate, shardId);
      executorShardToJob.put(shardId, getBaseJobTemplate(container, executorCommand));
    }

    LOG.info("Wait for jobScheduler's availability");
    startLatch = new CountDownLatch(1);
    if (mesosJobFramework.setSafeLatch(startLatch)) {
      try {
        if (!startLatch.await(MAX_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
          throw new RuntimeException("Job Scheduler does not recover in expected time!");
        }
      } catch (InterruptedException e) {
        throw new RuntimeException("Mesos Scheduler is interrupted:", e);
      }
    }
    for (BaseJob job : executorShardToJob.values()) {
      jobScheduler.registerJob(job);
    }
    LOG.info("All containers job have been submitted to new mesos job scheduler");
    LOG.info("The BaseJob Info: ");
    LOG.info(executorShardToJob.toString());
  }

  // Parse config and construct the job scheduler
  private JobScheduler getJobScheduler() {
    String rootPath =
        String.format("%s/%s",
            context.getPropertyWithException(MesosConfig.HERON_MESOS_FRAMEWORK_ZOOKEEPER_ROOT),
            topologyName);

    int connectionTimeoutMs =
        Integer.parseInt(context.getPropertyWithException(MesosConfig.HERON_MESOS_FRAMEWORK_ZOOKEEPER_CONNECT_TIMEOUT));
    int sessionTimeoutMs =
        Integer.parseInt(context.getPropertyWithException(MesosConfig.HERON_MESOS_FRAMEWORK_ZOOKEEPER_SESSION_TIMEOUT));

    persistenceStore = new ZkPersistenceStore(
        context.getPropertyWithException(MesosConfig.HERON_MESOS_FRAMEWORK_ZOOKEEPER_ENDPOINT),
        connectionTimeoutMs, sessionTimeoutMs,
        rootPath);

    FrameworkConfiguration frameworkConfig = FrameworkConfiguration.getFrameworkConfiguration();
    frameworkConfig.schedulerName = topologyName + "-framework";
    frameworkConfig.master = context.getPropertyWithException(MesosConfig.MESOS_MASTER_URI_PREFIX);
    frameworkConfig.user = context.getPropertyWithException(Constants.ROLE);

    frameworkConfig.failoverTimeoutSeconds =
        Integer.parseInt(
            context.getPropertyWithException(
                MesosConfig.HERON_MESOS_FRAMEWORK_FAILOVER_TIMEOUT_SECONDS));

    frameworkConfig.reconciliationIntervalInMs =
        Long.parseLong(
            context.getPropertyWithException(
                MesosConfig.HERON_MESOS_FRAMEWORK_RECONCILIATION_INTERVAL_MS));
    frameworkConfig.hostname = "";

    MesosTaskBuilder mesosTaskBuilder = new MesosTaskBuilder();

    mesosJobFramework = new MesosJobFramework(mesosTaskBuilder, persistenceStore, frameworkConfig);

    MesosDriverFactory mesosDriver = new MesosDriverFactory(mesosJobFramework, persistenceStore, frameworkConfig);
    JobScheduler jobScheduler = new JobScheduler(mesosJobFramework, persistenceStore, mesosDriver, frameworkConfig);

    return jobScheduler;
  }

  @Override
  public void schedule(PackingPlan packing) {
    // Start regular containers other than TMaster
    startRegularContainers(packing);
  }

  private BaseJob getBaseJobTemplate(PackingPlan.ContainerPlan container, String command) {
    BaseJob jobDef = new BaseJob();

    jobDef.name = "container_" + container.id + "_" + UUID.randomUUID();
    jobDef.command = command;

    jobDef.retries = Integer.MAX_VALUE;
    jobDef.owner = context.getPropertyWithException(Constants.ROLE);
    jobDef.runAsUser = context.getPropertyWithException(Constants.ROLE);
    jobDef.description = "Container for id: " + container.id + " for topology: " + topologyName;
    jobDef.cpu = container.resource.cpu;
    jobDef.disk = container.resource.disk / Constants.MB;
    jobDef.mem = container.resource.ram / Constants.MB;
    jobDef.shell = true;

    jobDef.uris = new ArrayList<>();
    String topologyPath = context.getProperty(Constants.TOPOLOGY_PKG_URI);
    String heronCoreReleasePath = context.getProperty(Constants.HERON_CORE_RELEASE_URI);
    jobDef.uris.add(topologyPath);
    jobDef.uris.add(heronCoreReleasePath);


    return jobDef;
  }

  private static String extractFilenameFromUri(String url) {
    return url.substring(url.lastIndexOf('/') + 1, url.length());
  }

  private String getExecutorCmdTemplate() {
    String topologyTarfile = extractFilenameFromUri(
        context.getProperty(Constants.TOPOLOGY_PKG_URI));
    String heronCoreFile = extractFilenameFromUri(
        context.getProperty(Constants.HERON_CORE_RELEASE_URI));

    String cmd = String.format(
        "rm %s %s && mkdir log-files && ./heron-executor",
         topologyTarfile, heronCoreFile);

    StringBuilder command = new StringBuilder(cmd);

    String executorArgs = getHeronJobExecutorArguments(context);

    command.append(" %d");
    command.append(" " + executorArgs);

    return command.toString();
  }

  @Override
  public void onHealthCheck(String healthCheckResponse) {

  }

  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) {
    for (BaseJob job : executorShardToJob.values()) {
      jobScheduler.deregisterJob(job.name, true);
    }

    CountDownLatch endLatch = new CountDownLatch(1);
    mesosJobFramework.setEndNotification(endLatch);

    LOG.info("Wait for the completeness of all mesos jobs");
    try {
      if (!endLatch.await(MAX_WAIT_TIMEOUT_MS, TimeUnit.MILLISECONDS)) {
        throw new RuntimeException("Job Scheduler does not kill all jobs in expected time!");
      }
    } catch (InterruptedException e) {
      throw new RuntimeException("Mesos Scheduler is interrupted:", e);
    }

    // Clear the frameworkId
    LOG.info("Mesos jobs killed; to clean up...");
    return persistenceStore.removeFrameworkID() && persistenceStore.clean();
  }

  @Override
  public boolean onActivate(Scheduler.ActivateTopologyRequest request) {
    LOG.info("To activate topology: " + request.getTopologyName());

    try {
      communicateTMaster("activate");
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "We could not deactivate topology:", e);
      return false;
    }
    return true;
  }

  @Override
  public boolean onDeactivate(Scheduler.DeactivateTopologyRequest request) {
    LOG.info("To deactivate topology: " + request.getTopologyName());

    try {
      communicateTMaster("deactivate");
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "We could not deactivate topology:", e);
      return false;
    }
    return true;
  }

  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    Integer shardId = request.getContainerIndex();
    BaseJob job = executorShardToJob.get(shardId);

    jobScheduler.restartJob(job.name);

    return true;
  }

  /**
   * Private methods
   */

  private void createTmasterRunScript() {
    // Create tmaster script.
    File tmasterRunScript = new File("run-tmaster.sh");
    if (tmasterRunScript.exists()) {
      tmasterRunScript.delete();
    }

    String heronTMasterArguments = getHeronTMasterArguments(context,
        Integer.parseInt(context.getPropertyWithException(MesosConfig.TMASTER_CONTROLLER_PORT)),
        Integer.parseInt(context.getPropertyWithException(MesosConfig.TMASTER_MAIN_PORT)),
        Integer.parseInt(context.getPropertyWithException(MesosConfig.TMASTER_STAT_PORT)),
        Integer.parseInt(context.getPropertyWithException(MesosConfig.TMASTER_SHELL_PORT)),
        Integer.parseInt(context.getPropertyWithException(MesosConfig.TMASTER_METRICSMGR_PORT)));

    try {
      FileWriter fw = new FileWriter(tmasterRunScript);
      fw.write("#!/bin/bash");
      fw.write(System.lineSeparator());
      fw.write("./heron-executor 0 " + heronTMasterArguments);
      fw.write(System.lineSeparator());
      fw.close();
      tmasterRunScript.setExecutable(true);
    } catch (IOException ioe) {
      LOG.log(Level.SEVERE, "Failed to create run-tmaster script", ioe);
    }
  }

  private void runTmaster() {
    while (tmasterRestart.get()) {
      LOG.info("Starting tmaster executor");

      try {
        tmasterProcess = ShellUtility.runASyncProcess(
            true, true, "./run-tmaster.sh", new File("."));
        int exitValue = tmasterProcess.waitFor();  // Block.
        LOG.log(Level.SEVERE, "Tmaster process exitted. Exit: " + exitValue);
      } catch (InterruptedException e) {
        try {
          int exitValue = tmasterProcess.exitValue();
          LOG.log(Level.SEVERE, "Tmaster process exitted. Exit: " + exitValue);
          continue;  // Try restarting.
        } catch (IllegalThreadStateException ex) {
          LOG.log(Level.SEVERE, "Thread interrupted", ex);
        }
      }
    }
  }

  /**
   * Communicate with TMaster with command
   *
   * @param command the command requested to TMaster, activate or deactivate.
   * @return true if the requested command is processed successfully by tmaster
   */
  protected boolean communicateTMaster(String command) {
    // Get the TMasterLocation
    TopologyMaster.TMasterLocation location =
        NetworkUtility.awaitResult(stateManager.getTMasterLocation(),
            5,
            TimeUnit.SECONDS);

    String endpoint = String.format("http://%s:%d/%s?topologyid=%s",
        location.getHost(),
        location.getControllerPort(),
        command,
        location.getTopologyId());

    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) new URL(endpoint).openConnection();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to get connection to tmaster: ", e);
      return false;
    }

    NetworkUtility.sendHttpGetRequest(connection);

    try {
      if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
        LOG.info("Successfully  " + command);
        return true;
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to " + command + " :", e);
    } finally {
      connection.disconnect();
    }

    return true;
  }

  public static String getHeronJobExecutorArguments(LaunchContext context) {
    return getHeronExecutorArguments(context,
        "{{task.ports[STMGR_PORT]}}", "{{task.ports[METRICMGR_PORT]}}",
        "{{task.ports[BACK_UP]}}", "{{task.ports[SHELL]}}",
        "{{task.ports[BACK_UP_1]}}");
  }

  public static String getHeronExecutorArguments(
      LaunchContext context,
      String sPort1,
      String sPort2,
      String sPort3,
      String sPort4,
      String sPort5) {

    return String.format(
        "\"%s\" \"%s\" \"%s\" " +
            "\"%s\" \"%s\" \"%s\" " +
            "\"%s\" \"%s\" \"%s\" " +
            "\"%s\" \"%s\" \"%s\" " +
            "\"%s\" \"%s\" \"%s\" " +
            "\"%s\" \"%s\" \"%s\" " +
            "\"%s\" \"%s\" \"%s\" " +
            "\"%s\" \"%s\" \"%s\"",
        context.getProperty("TOPOLOGY_NAME"), context.getProperty("TOPOLOGY_ID"), context.getProperty("TOPOLOGY_DEFN"),
        context.getProperty("INSTANCE_DISTRIBUTION"), context.getProperty("ZK_NODE"), context.getProperty("ZK_ROOT"),
        context.getProperty("TMASTER_BINARY"), context.getProperty("STMGR_BINARY"), context.getProperty("METRICS_MGR_CLASSPATH"),
        context.getProperty("INSTANCE_JVM_OPTS_IN_BASE64"), context.getProperty("CLASSPATH"), sPort1,
        sPort2, sPort3, context.getProperty("HERON_INTERNALS_CONFIG_FILENAME"),
        context.getProperty("COMPONENT_RAMMAP"), context.getProperty("COMPONENT_JVM_OPTS_IN_BASE64"), context.getProperty("PKG_TYPE"),
        context.getProperty("TOPOLOGY_JAR_FILE"), context.getProperty("HERON_JAVA_HOME"),
        sPort4, context.getProperty("LOG_DIR"), context.getProperty("HERON_SHELL_BINARY"),
        sPort5);

  }

  public static String getHeronTMasterArguments(
      LaunchContext context,
      int port1,  // Port for TMaster Controller and Stream-manager port
      int port2,  // Port for TMaster and Stream-manager communication amd Stream manager with
      // metric manager communicaiton.
      int port3,   // Port for TMaster stats export. Used by tracker.
      int shellPort,  // Port for heorn-shell
      int metricsmgrPort      // Port for MetricsMgr in TMasterContainer
  ) {
    return getHeronExecutorArguments(context, "" + port1, "" + port2,
        "" + port3, "" + shellPort, "" + metricsmgrPort);
  }
}
