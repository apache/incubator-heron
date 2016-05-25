package com.twitter.heron.scheduler.mesos;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.scheduler.mesos.framework.config.FrameworkConfiguration;
import com.twitter.heron.scheduler.mesos.framework.driver.MesosDriverFactory;
import com.twitter.heron.scheduler.mesos.framework.driver.MesosJobFramework;
import com.twitter.heron.scheduler.mesos.framework.driver.MesosTaskBuilder;
import com.twitter.heron.scheduler.mesos.framework.jobs.BaseJob;
import com.twitter.heron.scheduler.mesos.framework.jobs.JobScheduler;
import com.twitter.heron.scheduler.mesos.framework.state.PersistenceStore;
import com.twitter.heron.scheduler.mesos.framework.state.ZkPersistenceStore;
import com.twitter.heron.spi.common.*;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.SchedulerUtils;
import com.twitter.heron.spi.utils.TopologyUtils;

import javax.xml.bind.DatatypeConverter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public class MesosScheduler implements IScheduler {
  private static final Logger LOG = Logger.getLogger(MesosScheduler.class.getName());

  private static final long MAX_WAIT_TIMEOUT_MS = 30 * 1000;

  private Config config;
  private Config runtime;
  private PackingPlan packing;
  private AtomicBoolean tmasterRestart;
  private Process tmasterProcess;
  private String topologyPackageUri;
  private String corePackageURI;

  private TopologyAPI.Topology topology;

  private JobScheduler jobScheduler;

  private final Map<Integer, BaseJob> executorShardToJob = new ConcurrentHashMap<>();

  private PersistenceStore persistenceStore;

  private MesosJobFramework mesosJobFramework;

  private void startRegularContainers() {

    LOG.info("We are to start the new mesos job scheduler now");
    this.topology = Runtime.topology(runtime);

    int shardId = 0;
    String executorCmdTemplate = getExecutorCmdTemplate();

    for (PackingPlan.ContainerPlan container : packing.containers.values()) {
      shardId++;
      String executorCommand = String.format(executorCmdTemplate, shardId);
      executorShardToJob.put(shardId, getBaseJobTemplate(container, executorCommand));
    }

    LOG.info("Wait for jobScheduler's availability");
    CountDownLatch startLatch = new CountDownLatch(1);
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
            MesosContext.frameworkZKRoot(config),
            Runtime.topologyName(runtime));

    int connectionTimeoutMs =
        MesosContext.frameworkZKConnectTimeout(config);
    int sessionTimeoutMs =
        MesosContext.frameworkZKSessionTimeout(config);

    persistenceStore = new ZkPersistenceStore(
        MesosContext.frameworkZKEndpoint(config),
        connectionTimeoutMs, sessionTimeoutMs,
        rootPath);

    FrameworkConfiguration frameworkConfig = FrameworkConfiguration.getFrameworkConfiguration();
    frameworkConfig.schedulerName = Runtime.topologyName(runtime) + "-framework";
    frameworkConfig.master = MesosContext.mesosMasterURI(config);
    frameworkConfig.user = Context.role(config);

    frameworkConfig.failoverTimeoutSeconds = MesosContext.failoverTimeoutSeconds(config);

    frameworkConfig.reconciliationIntervalInMs = MesosContext.reconciliationIntervalMS(config);
    frameworkConfig.hostname = "";

    MesosTaskBuilder mesosTaskBuilder = new MesosTaskBuilder();

    mesosJobFramework = new MesosJobFramework(mesosTaskBuilder, persistenceStore, frameworkConfig);

    MesosDriverFactory mesosDriver = new MesosDriverFactory(mesosJobFramework, persistenceStore, frameworkConfig);
    JobScheduler jobScheduler = new JobScheduler(mesosJobFramework, persistenceStore, mesosDriver, frameworkConfig);

    return jobScheduler;
  }

  @Override
  public boolean onSchedule(PackingPlan packing) {
    this.packing = packing;
    // Start tmaster.
    createTmasterRunScript();
    Thread tmasterRunThread = new Thread(new Runnable() {
      @Override
      public void run() {
        runTmaster();
      }
    });
    tmasterRunThread.setDaemon(true);
    tmasterRunThread.start();

    // Start regular containers other than TMaster
    startRegularContainers();
    return true;
  }

  private BaseJob getBaseJobTemplate(PackingPlan.ContainerPlan container, String command) {
    BaseJob jobDef = new BaseJob();

    jobDef.name = "container_" + container.id + "_" + UUID.randomUUID();
    jobDef.command = command;

    jobDef.retries = Integer.MAX_VALUE;
    jobDef.owner = Context.role(config);
    jobDef.runAsUser = Context.role(config);
    jobDef.description = "Container for id: " + container.id + " for topology: " + Runtime.topologyName(runtime);
    jobDef.cpu = container.resource.cpu;
    jobDef.disk = container.resource.disk / Constants.MB;
    jobDef.mem = container.resource.ram / Constants.MB;
    jobDef.shell = true;

    jobDef.uris = new ArrayList<>();
    String topologyPath = topologyPackageUri;
    String heronCoreReleasePath = corePackageURI;
    jobDef.uris.add(topologyPath);
    jobDef.uris.add(heronCoreReleasePath);


    return jobDef;
  }

  private static String extractFilenameFromUri(String url) {
    return url.substring(url.lastIndexOf('/') + 1, url.length());
  }

  private String getExecutorCmdTemplate() {
    String topologyTarfile = extractFilenameFromUri(
        topologyPackageUri);
    String heronCoreFile = extractFilenameFromUri(
        corePackageURI);

    String cmd = String.format(
        "tar -xvf %s && rm %s %s && %s",
        topologyTarfile, topologyTarfile, heronCoreFile, Context.executorSandboxBinary(config));

    StringBuilder command = new StringBuilder(cmd);

    String executorArgs = getHeronJobExecutorArguments();

    command.append(" %d");
    command.append(" " + executorArgs);

    return command.toString();
  }

  @Override
  public void initialize(Config config, Config runtime) {
    LOG.info("Initializing new mesos topology scheduler");
    this.tmasterRestart = new AtomicBoolean(true);
    this.config = config;
    this.runtime = runtime;

    // Start the jobScheduler
    this.jobScheduler = getJobScheduler();
    this.jobScheduler.start();
    //TODO: should be a temporary workaround
    this.topologyPackageUri = System.getenv("TOPOLOGY_PACKAGE_URI");
    this.corePackageURI = System.getenv("CORE_PACKAGE_URI");
  }

  @Override
  public void close() {
  }

  @Override
  public List<String> getJobLinks() {
    return null;
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
    mesosJobFramework.killFramework();
    persistenceStore.removeFrameworkID();
    persistenceStore.clean();
    tmasterRestart.set(false);
    tmasterProcess.destroy();
    //TODO: also need some delayed shutdown after giving away response, so this process itself would shutdown
    return true;
  }

  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    //TODO: create a new implementation

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

    List<Integer> freePorts = new ArrayList<>(SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR);
    for (int i = 0; i < SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR; i++) {
      freePorts.add(SysUtils.getFreePort());
    }

    String[] executorCmd = SchedulerUtils.executorCommand(config, runtime, 0, freePorts);

    try {
      FileWriter fw = new FileWriter(tmasterRunScript);
      fw.write("#!/bin/bash");
      fw.write(System.lineSeparator());

      fw.write(String.join(" ", executorCmd));
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
        tmasterProcess = ShellUtils.runASyncProcess(
            true, "./run-tmaster.sh", new File(""));
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

  public String getHeronJobExecutorArguments() {
    return getHeronExecutorArguments(
        "{{task.ports[STMGR_PORT]}}", "{{task.ports[METRICMGR_PORT]}}",
        "{{task.ports[BACK_UP]}}", "{{task.ports[SHELL]}}",
        "{{task.ports[BACK_UP_1]}}", "{{task.ports[SCHEDULER_PORT]}}");
  }

  public String getHeronExecutorArguments(
      String sPort1,
      String sPort2,
      String sPort3,
      String sPort4,
      String sPort5,
      String sPort6) {

    return String.format(
        "\"%s\" \"%s\" \"%s\" " +
            "\"%s\" \"%s\" \"%s\" " +
            "\"%s\" \"%s\" \"%s\" " +
            "\"%s\" \"%s\" \"%s\" " +
            "\"%s\" \"%s\" \"%s\" " +
            "\"%s\" \"%s\" \"%s\" " +
            "\"%s\" \"%s\" \"%s\" " +
            "\"%s\" \"%s\" \"%s\" " +
            "\"%s\" \"%s\" \"%s\" " +
            "\"%s\" \"%s\" \"%s\"",
        Runtime.topologyName(runtime), Runtime.topologyId(runtime), FileUtils.getBaseName(Context.topologyDefinitionFile(config)),
        packing.getInstanceDistribution(), MesosContext.stateManagerConnectionString(config), MesosContext.stateManagerRootPath(config),
        Context.tmasterSandboxBinary(config), Context.stmgrSandboxBinary(config), Context.metricsManagerSandboxClassPath(config),
        formatJavaOpts(TopologyUtils.getInstanceJvmOptions(topology)), TopologyUtils.makeClassPath(topology, Context.topologyJarFile(config)), sPort1,
        sPort2, sPort3, Context.systemConfigSandboxFile(config), TopologyUtils.formatRamMap(
            TopologyUtils.getComponentRamMap(topology, Context.instanceRam(config))),
        formatJavaOpts(TopologyUtils.getComponentJvmOptions(topology)), Context.topologyPackageType(config),
        FileUtils.getBaseName(Context.topologyJarFile(config)), Context.javaSandboxHome(config),
        sPort4, Context.shellSandboxBinary(config), sPort5, Context.cluster(config), Context.role(config),
        Context.environ(config), Context.instanceSandboxClassPath(config), Context.metricsSinksSandboxFile(config),
        completeSchedulerClasspath(), sPort6);
  }

  public String getHeronTMasterArguments(
      Config config,
      PackingPlan packing,
      int port1,  // Port for TMaster Controller and Stream-manager port
      int port2,  // Port for TMaster and Stream-manager communication amd Stream manager with
      // metric manager communicaiton.
      int port3,   // Port for TMaster stats export. Used by tracker.
      int shellPort,  // Port for heorn-shell
      int metricsmgrPort,      // Port for MetricsMgr in TMasterContainer
      int schedulerPort      // Port for MetricsMgr in TMasterContainer
  ) {
    return getHeronExecutorArguments("" + port1, "" + port2,
        "" + port3, "" + shellPort, "" + metricsmgrPort, "" + schedulerPort);
  }

  protected String formatJavaOpts(String javaOpts) {
    String javaOptsBase64 = DatatypeConverter.printBase64Binary(
        javaOpts.getBytes(Charset.forName("UTF-8")));

    return String.format("\"%s\"", javaOptsBase64.replace("=", "&equals;"));
  }

  private String completeSchedulerClasspath() {
    return new StringBuilder()
        .append(Context.schedulerSandboxClassPath(config)).append(":")
        .append(Context.packingSandboxClassPath(config)).append(":")
        .append(Context.stateManagerSandboxClassPath(config))
        .toString();
  }
}
