package com.twitter.heron.scheduler.local;

import java.io.File;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.proto.scheduler.Scheduler;
import com.twitter.heron.proto.tmaster.TopologyMaster;

import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.common.ShellUtils;
import com.twitter.heron.spi.common.HttpUtils;

import com.twitter.heron.spi.utils.NetworkUtils;
import com.twitter.heron.spi.utils.Runtime;

import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.statemgr.IStateManager;

import com.google.common.util.concurrent.ListenableFuture;

public class LocalScheduler implements IScheduler {
  private static final Logger LOG = Logger.getLogger(LocalScheduler.class.getName());

  private Config config;
  private Config runtime;

  // executor service for monitoring all the containers
  private final ExecutorService monitorService = Executors.newCachedThreadPool();

  // map to keep track of the process and the shard it is running
  private final Map<Process, Integer> processToContainer = new ConcurrentHashMap<Process, Integer>();

  // Has the topology been killed?
  private volatile boolean topologyKilled = false;

  @Override
  public void initialize(Config config, Config runtime) {
    this.config = config;
    this.runtime = runtime;

    LOG.info("Starting to deploy topology: " + Context.topologyName(config));
    LOG.info("# of containers: " + Runtime.numContainers(config));

    // first, run the TMaster executor
    // startExecutor(0);
    // LOG.info("TMaster is started.");

    // for each container, run its own executor
    int numContainers = Integer.parseInt(Runtime.numContainers(config));
    for (int i = 1; i < numContainers; i++) {
      startExecutor(i);
    }

    LOG.info("Executor for each container have been started.");
  }

  /**
   * Start the executor for the given container
   */
  protected void startExecutor(final int container) {
    LOG.info("Starting a new executor for container: " + container);

    // create a process with the executor command and topology working directory
    final Process regularExecutor = ShellUtils.runASyncProcess(true, true, 
        (String)null,
        // TO DO getExecutorCmd(container),
        new File(LocalContext.workingDirectory(config)));

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
    String topologyName = Context.topologyName(config);
    LOG.info("Command to kill topology: "  + topologyName);

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
    IStateManager stateManager = Runtime.stateManager(runtime);

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
    LOG.info("Command to activate topology: "  + Context.topologyName(config));
    return sendToTMaster("activate");
  }

  /**
   * Handler to deactivate topology
   */
  @Override
  public boolean onDeactivate(Scheduler.DeactivateTopologyRequest request) {
    LOG.info("Command to deactivate topology: "  + Context.topologyName(config));
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
      LOG.info("Command to restart the entire topology: "  + Context.topologyName(config));

      // create a new tmp list to store all the Process to be killed
      List<Process> tmpProcessList = new LinkedList<>(processToContainer.keySet());
      for (Process process : tmpProcessList) {
        process.destroy();
      }
    } else {
      // restart that particular container
      LOG.info("Command to restart a container of topology: "  + Context.topologyName(config));
      LOG.info("Restart container requested: " + containerId);

      // locate the container and destroy it
      for (Process p : processToContainer.keySet()) {
        if (containerId == processToContainer.get(p)) {
          p.destroy();
        }
      }
    }

    // get the instance of state manager to clean state
    IStateManager stateManager = Runtime.stateManager(runtime);

    // Clean TMasterLocation since we could not set it as ephemeral for local file system
    // We would not clean SchedulerLocation since we would not restart the Scheduler
    stateManager.deleteTMasterLocation(Context.topologyName(config));

    return true;
  }

  /*
  private String getExecutorCmd(int container) {
    int port1 = NetworkUtils.getFreePort();
    int port2 = NetworkUtils.getFreePort();
    int port3 = NetworkUtils.getFreePort();
    int shellPort = NetworkUtils.getFreePort();
    int port4 = NetworkUtils.getFreePort();

    if (port1 == -1 || port2 == -1 || port3 == -1) {
      throw new RuntimeException("Could not find available ports to start topology");
    }

    return String.format("./%s %d %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %d %s %s %d",
        localConfig.getExecutorBinary(),
        container,
        localConfig.getTopologyName(),
        localConfig.getTopologyId(),
        localConfig.getTopologyDef(),
        localConfig.getInstanceDistribution(),
        localConfig.getZkNode(),
        localConfig.getZkRoot(),
        localConfig.getTMasterBinary(),
        localConfig.getStmgrBinary(),
        localConfig.getMetricsMgrClasspath(),
        localConfig.getInstanceJvmOptionsBase64(),
        localConfig.getClasspath(),
        port1,
        port2,
        port3,
        localConfig.getInternalConfigFilename(),
        localConfig.getComponentRamMap(),
        localConfig.getComponentJVMOptionsBase64(),
        localConfig.getPkgType(),
        localConfig.getTopologyJarFile(),
        localConfig.getJavaHome(),
        shellPort,
        localConfig.getLogDir(),
        localConfig.getHeronShellBinary(),
        port4
    );
  }
  */

  /**
   * Communicate with TMaster with command
   *
   * @param command the command requested to TMaster, activate or deactivate.
   * @return true if the requested command is processed successfully by tmaster
   */
  protected boolean sendToTMaster(String command) {
    // get the topology name
    String topologyName = Context.topologyName(config);

    // get the state manager instance
    IStateManager stateManager = Runtime.stateManager(runtime);

    // fetch the TMasterLocation for the topology
    LOG.info("Fetching TMaster location for topology: " + topologyName);
    ListenableFuture<TopologyMaster.TMasterLocation> locationFuture = 
        stateManager.getTMasterLocation(null, Context.topologyName(config));

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

    // get the response and check if it is successful
    try {
      if (connection.getResponseCode() == HttpURLConnection.HTTP_OK) {
        LOG.info("Successfully got a HTTP response from TMaster for " + command);
        return true;
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to receive HTTP response from TMaster for " + command + " :", e);
    } finally {
      connection.disconnect();
    }

    return true;
  }
}
