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

import com.twitter.heron.spi.common.PackingPlan;
import com.twitter.heron.spi.scheduler.IScheduler;
import com.twitter.heron.spi.scheduler.SchedulerStateManagerAdaptor;
import com.twitter.heron.spi.scheduler.context.LaunchContext;

import com.twitter.heron.scheduler.util.NetworkUtility;
import com.twitter.heron.scheduler.util.ShellUtility;

public class LocalScheduler implements IScheduler {
  private static final Logger LOG = Logger.getLogger(LocalScheduler.class.getName());

  private LaunchContext context;
  private LocalConfig localConfig;

  private final ExecutorService monitorServices = Executors.newCachedThreadPool();

  private final Map<Process, Integer> processToShard = new ConcurrentHashMap<Process, Integer>();

  private volatile boolean topologyKilled = false;

  private SchedulerStateManagerAdaptor stateManager;

  @Override
  public void initialize(LaunchContext context) {
    this.context = context;
    this.localConfig = new LocalConfig(context);
    this.stateManager = context.getStateManagerAdaptor();

    LOG.info("Now start to deploy topology");
    LOG.info("# of shard: " + this.localConfig.getNumShard());

    // 1. Run TMaster executor first
    startExecutor(0);

    LOG.info("TMaster is started.");

    // 2. Run regular executor
    int numShards = Integer.parseInt(this.localConfig.getNumShard());
    for (int i = 1; i < numShards; i++) {
      startExecutor(i);
    }

    LOG.info("Regular executors are started.");
  }

  protected void startExecutor(final int shard) {
    LOG.info("Starting a new executor: " + shard);

    final Process regularExecutor = ShellUtility.runASyncProcess(true, true, getExecutorCmd(shard),
        new File(this.localConfig.getWorkingDirectory()));

    processToShard.put(regularExecutor, shard);
    LOG.info("Started new Executor " + shard);

    // Add to the monitorServices
    Runnable r = new Runnable() {
      @Override
      public void run() {
        try {
          LOG.info("Waiting for shard " + shard + " to finish.");
          regularExecutor.waitFor();
          LOG.info("Shard " + shard + " is done. Exit status: " + regularExecutor.exitValue());

          if (topologyKilled) {
            LOG.info("Topology is killed. Not to start new executors.");
            return;
          }

          startExecutor(processToShard.remove(regularExecutor));
        } catch (InterruptedException e) {
          LOG.log(Level.SEVERE, "Process is interrupted: ", e);
        }
      }
    };

    monitorServices.submit(r);
  }

  @Override
  public void schedule(PackingPlan packing) {
    // We do nothing here
  }

  @Override
  public void onHealthCheck(String healthCheckResponse) {
    // TODO:-
  }

  @Override
  public boolean onKill(Scheduler.KillTopologyRequest request) {
    LOG.info("To kill topology: " + localConfig.getTopologyName());

    // Set the flag first
    topologyKilled = true;

    // Destroy the process
    for (Process p : processToShard.keySet()) {
      int index = processToShard.get(p);
      LOG.info("Killing executor: " + index);
      p.destroy();
      LOG.info("Killed executor: " + index);
    }

    processToShard.clear();

    LOG.info("To clear Scheduler Location");
    stateManager.clearSchedulerLocation();
    LOG.info("Cleared Scheduler Location");

    LOG.info("To clear TMaster Location");
    stateManager.clearTMasterLocation();
    LOG.info("Cleared Scheduler Location");


    return true;
  }

  @Override
  public boolean onActivate(Scheduler.ActivateTopologyRequest request) {
    LOG.info("To activate topology: " + localConfig.getTopologyName());

    return communicateTMaster("activate");
  }

  @Override
  public boolean onDeactivate(Scheduler.DeactivateTopologyRequest request) {
    LOG.info("To deactivate topology: " + localConfig.getTopologyName());

    return communicateTMaster("deactivate");
  }

  @Override
  public boolean onRestart(Scheduler.RestartTopologyRequest request) {
    // Container would be restarted automatically once we destroy it
    int shardId = request.getContainerIndex();

    if (shardId == -1) {
      LOG.info("To restart all containers.");
      // We would create a new tmp list to store Process to be killed

      List<Process> tmpProcessList = new LinkedList<>(processToShard.keySet());
      for (Process process : tmpProcessList) {
        process.destroy();
      }
    } else {
      // restart that particular container
      LOG.info("To restart shard: " + shardId);
      for (Process p : processToShard.keySet()) {
        if (shardId == processToShard.get(p)) {
          p.destroy();
        }
      }
    }

    // Clean TMasterLocation since we could not set it ephemeral in local file system
    // We would not clean SchedulerLocation since we would not restart the Scheduler
    stateManager.clearTMasterLocation();

    return true;
  }

  private String getExecutorCmd(int shard) {
    int port1 = NetworkUtility.getFreePort();
    int port2 = NetworkUtility.getFreePort();
    int port3 = NetworkUtility.getFreePort();
    int shellPort = NetworkUtility.getFreePort();
    int port4 = NetworkUtility.getFreePort();

    if (port1 == -1 || port2 == -1 || port3 == -1) {
      throw new RuntimeException("Could not find available ports to start topology");
    }

    return String.format("./%s %d %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %s %d %s %s %d",
        localConfig.getExecutorBinary(),
        shard,
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
}
