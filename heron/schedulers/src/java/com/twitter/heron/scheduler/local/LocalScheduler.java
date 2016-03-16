package com.twitter.heron.scheduler.local;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import com.google.common.util.concurrent.ListenableFuture;

import org.apache.commons.io.FilenameUtils;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.HttpUtils;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.common.ShellUtils;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.statemgr.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.utils.NetworkUtils;
import com.twitter.heron.spi.utils.Runtime;
import com.twitter.heron.spi.utils.TopologyUtils;

public class LocalScheduler implements IScheduler {
  private static final Logger LOG = Logger.getLogger(LocalScheduler.class.getName());

  private Config config;
  private Config runtime;

  // executor service for monitoring all the containers
  private final ExecutorService monitorService = Executors.newCachedThreadPool();

  // map to keep track of the process and the shard it is running
  private final Map<Process, Integer> processToContainer = new ConcurrentHashMap<Process, Integer>();

  // has the topology been killed?
  private volatile boolean topologyKilled = false;

  @Override
  public void initialize(Config config, Config runtime) {
    this.config = config;
    this.runtime = runtime;

    LOG.info("Starting to deploy topology: " + LocalContext.topologyName(config));
    LOG.info("# of containers: " + Runtime.numContainers(runtime));

    // first, run the TMaster executor
    startExecutor(0);
    LOG.info("TMaster is started.");

    // for each container, run its own executor
    long numContainers = Runtime.numContainers(runtime);
    for (int i = 1; i < numContainers; i++) {
      startExecutor(i);
    }

    LOG.info("Executor for each container have been started.");
  }

  /**
   * Encode the JVM options
   *
   * @return encoded string
   */
  protected String formatJavaOpts(String javaOpts) {
    String javaOptsBase64 = DatatypeConverter.printBase64Binary(
        javaOpts.getBytes(Charset.forName("UTF-8")));

    return String.format("\"%s\"", javaOptsBase64.replace("=", "&equals;"));
  }

  /**
   * Start the executor for the given container
   */
  protected void startExecutor(final int container) {
    LOG.info("Starting a new executor for container: " + container);

    // create a process with the executor command and topology working directory
    final Process regularExecutor = ShellUtils.runASyncProcess(true,
        getExecutorCommand(container), new File(LocalContext.workingDirectory(config)));

    // associate the process and its container id
    processToContainer.put(regularExecutor, container);
    LOG.info("Started the executor for container: " + container);

    // add the container for monitoring
    Runnable r = new Runnable() {
      @Override
      public void run() {
        try {
          LOG.info("Waiting for container " + container + " to finish.");
          regularExecutor.waitFor();

          LOG.info("Container " + container + " is completed. Exit status: " + regularExecutor.exitValue());
          if (topologyKilled) {
            LOG.info("Topology is killed. Not to start new executors.");
            return;
          }

          // restart the container
          startExecutor(processToContainer.remove(regularExecutor));
        } catch (InterruptedException e) {
          LOG.log(Level.SEVERE, "Process is interrupted: ", e);
        }
      }
    };

    monitorService.submit(r);
  }

  private String getExecutorCommand(int container) {

    TopologyAPI.Topology topology = Runtime.topology(runtime);

    int port1 = NetworkUtils.getFreePort();
    int port2 = NetworkUtils.getFreePort();
    int port3 = NetworkUtils.getFreePort();
    int shellPort = NetworkUtils.getFreePort();
    int port4 = NetworkUtils.getFreePort();

    if (port1 == -1 || port2 == -1 || port3 == -1) {
      throw new RuntimeException("Could not find available ports to start topology");
    }

    String executorCmd = String.format("%s %d %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %d %s %s %d %s %s %s %s %s %s %d",
        LocalContext.executorBinary(config),
        container,
        topology.getName(),
        topology.getId(),
        FilenameUtils.getName(LocalContext.topologyDefinitionFile(config)),
        Runtime.instanceDistribution(runtime),
        LocalContext.stateManagerConnectionString(config),
        LocalContext.stateManagerRootPath(config),
        LocalContext.tmasterBinary(config),
        LocalContext.stmgrBinary(config),
        LocalContext.metricsManagerClassPath(config),
        formatJavaOpts(TopologyUtils.getInstanceJvmOptions(topology)),
        TopologyUtils.makeClassPath(topology, LocalContext.topologyJarFile(config)),
        port1,
        port2,
        port3,
        LocalContext.systemConfigFile(config),
        TopologyUtils.formatRamMap(
            TopologyUtils.getComponentRamMap(topology, LocalContext.instanceRam(config))),
        formatJavaOpts(TopologyUtils.getComponentJvmOptions(topology)),
        LocalContext.topologyPackageType(config),
        LocalContext.topologyJarFile(config),
        LocalContext.javaHome(config),
        shellPort,
        LocalContext.logDirectory(config),
        LocalContext.shellBinary(config),
        port4,
        LocalContext.cluster(config),
        LocalContext.role(config),
        LocalContext.environ(config),
        LocalContext.instanceClassPath(config),
        LocalContext.metricsSinksFile(config),
        "no_need_since_scheduler_is_started",
        0
    );

    LOG.info("Executor command line: " + executorCmd.toString());
    return executorCmd;
  }

  /**
   * Schedule the provided packed plan
   */
  @Override
  public void schedule(PackingPlan packing) {
  }

  /**
   * Handler to kill topology
   */
  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) {

    // get the topology name
    String topologyName = LocalContext.topologyName(config);
    LOG.info("Command to kill topology: " + topologyName);

    // set the flag that the topology being killed
    topologyKilled = true;

    // destroy/kill the process for each container
    for (Process p : processToContainer.keySet()) {

      // get the container index for the process
      int index = processToContainer.get(p);
      LOG.info("Killing executor for container: " + index);

      // destroy the process
      p.destroy();
      LOG.info("Killed executor for container: " + index);
    }

    // clear the mapping between process and container ids
    processToContainer.clear();

    // get the state manager instance
    SchedulerStateManagerAdaptor stateManager = Runtime.schedulerStateManagerAdaptor(runtime);

    // remove the scheduler location for the topology from state manager
    LOG.info("Removing scheduler location for topology: " + topologyName);
    stateManager.deleteSchedulerLocation(topologyName);
    LOG.info("Cleared scheduler location for topology: " + topologyName);

    // remove the tmaster location for the topology from state manager
    LOG.info("Removing TMaster location for topology: " + topologyName);
    stateManager.deleteTMasterLocation(topologyName);
    LOG.info("Cleared TMaster location for topology: " + topologyName);

    return true;
  }

  /**
   * Handler to activate topology
   */
  @Override
  public boolean onActivate(Scheduler.ActivateTopologyRequest request) {
    LOG.info("Command to activate topology: " + LocalContext.topologyName(config));
    return sendToTMaster("activate");
  }

  /**
   * Handler to deactivate topology
   */
  @Override
  public boolean onDeactivate(Scheduler.DeactivateTopologyRequest request) {
    LOG.info("Command to deactivate topology: " + LocalContext.topologyName(config));
    return sendToTMaster("deactivate");
  }

  /**
   * Handler to restart topology
   */
  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    // Containers would be restarted automatically once we destroy it
    int containerId = request.getContainerIndex();

    if (containerId == -1) {
      LOG.info("Command to restart the entire topology: " + LocalContext.topologyName(config));

      // create a new tmp list to store all the Process to be killed
      List<Process> tmpProcessList = new LinkedList<>(processToContainer.keySet());
      for (Process process : tmpProcessList) {
        process.destroy();
      }
    } else {
      // restart that particular container
      LOG.info("Command to restart a container of topology: " + LocalContext.topologyName(config));
      LOG.info("Restart container requested: " + containerId);

      // locate the container and destroy it
      for (Process p : processToContainer.keySet()) {
        if (containerId == processToContainer.get(p)) {
          p.destroy();
        }
      }
    }

    // get the instance of state manager to clean state
    SchedulerStateManagerAdaptor stateManager = Runtime.schedulerStateManagerAdaptor(runtime);

    // If we restart the container including TMaster, wee need to clean TMasterLocation,
    // since we could not set it as ephemeral for local file system
    // We would not clean SchedulerLocation since we would not restart the Scheduler
    if (containerId == -1 || containerId == 0) {
      stateManager.deleteTMasterLocation(LocalContext.topologyName(config));
    }

    return true;
  }

  /**
   * Communicate with TMaster with command
   *
   * @param command the command requested to TMaster, activate or deactivate.
   * @return true if the requested command is processed successfully by tmaster
   */
  protected boolean sendToTMaster(String command) {
    // get the topology name
    String topologyName = LocalContext.topologyName(config);

    // get the state manager instance
    SchedulerStateManagerAdaptor stateManager = Runtime.schedulerStateManagerAdaptor(runtime);

    // fetch the TMasterLocation for the topology
    LOG.info("Fetching TMaster location for topology: " + topologyName);
    ListenableFuture<TopologyMaster.TMasterLocation> locationFuture =
        stateManager.getTMasterLocation(null, LocalContext.topologyName(config));

    TopologyMaster.TMasterLocation location =
        NetworkUtils.awaitResult(locationFuture, 5, TimeUnit.SECONDS);
    LOG.info("Fetched TMaster location for topology: " + topologyName);

    // for the url request to be sent to TMaster
    String endpoint = String.format("http://%s:%d/%s?topologyid=%s",
        location.getHost(), location.getControllerPort(), command, location.getTopologyId());
    LOG.info("HTTP URL for TMaster: " + endpoint);

    // create a URL connection
    HttpURLConnection connection = null;
    try {
      connection = (HttpURLConnection) new URL(endpoint).openConnection();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to get a HTTP connection to TMaster: ", e);
      return false;
    }
    LOG.info("Successfully opened HTTP connection to TMaster");

    // now sent the http request
    HttpUtils.sendHttpGetRequest(connection);
    LOG.info("Sent the HTTP payload to TMaster");

    boolean success = false;
    // get the response and check if it is successful
    try {
      int responseCode = connection.getResponseCode();
      if (responseCode == HttpURLConnection.HTTP_OK) {
        LOG.info("Successfully got a HTTP response from TMaster for " + command);
        success = true;
      } else {
        LOG.info(String.format("Non OK HTTP response %d from TMaster for command %s",
          responseCode, command));
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to receive HTTP response from TMaster for " + command + " :", e);
    } finally {
      connection.disconnect();
    }

    return success;
  }
}
