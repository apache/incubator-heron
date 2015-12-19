package com.twitter.heron.scheduler.aurora;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.scheduler.api.Constants;
import com.twitter.heron.scheduler.api.IRuntimeManager;
import com.twitter.heron.scheduler.api.SchedulerStateManagerAdaptor;
import com.twitter.heron.scheduler.api.context.RuntimeManagerContext;
import com.twitter.heron.scheduler.util.ShellUtility;
import com.twitter.heron.scheduler.util.TopologyUtility;

/**
 * Handles Runtime tasks like kill/restart/activate/deactivate for heron topology launched
 * in aurora.
 */
public class AuroraTopologyRuntimeManager implements IRuntimeManager {
  private static final Logger LOG = Logger.getLogger(AuroraTopologyRuntimeManager.class.getName());
  private static final String HERON_BATCH_SIZE = "heron.batch.size";
  private RuntimeManagerContext context;
  private String topologyName;
  private String dc;
  private String role;
  private String environ;

  private void writeAuroraFile() {
    InputStream is = this.getClass().getResourceAsStream("/heron_controller.aurora");
    File auroraFile = new File("heron_controller.aurora");
    if (auroraFile.exists()) {
      auroraFile.delete();
    }
    try (FileWriter fw = new FileWriter(auroraFile)) {
      fw.write(ShellUtility.inputstreamToString(is));
      auroraFile.deleteOnExit();
    } catch (IOException e) {
      LOG.severe("Couldn't find heron_controller.aurora in classpath.");
      throw new RuntimeException("Couldn't find heron.aurora in classpath");
    }
  }

  protected boolean launchAuroraJob(Map<String, String> args) {
    ArrayList<String> auroraCmd = new ArrayList<>(Arrays.asList(
        "aurora", "job", "create", "--wait-until", "FINISHED"));
    for (String binding : args.keySet()) {
      auroraCmd.add("--bind");
      auroraCmd.add(String.format("%s=%s", binding, args.get(binding)));
    }
    auroraCmd.add(String.format("%s/%s/%s/%s",
        dc, role, "devel", args.get("JOB_NAME")));
    auroraCmd.add("heron_controller.aurora");
    if (context.isVerbose()) {
      auroraCmd.add("--verbose");
    }
    if (0 != ShellUtility.runProcess(
        true, auroraCmd.toArray(new String[auroraCmd.size()]),
        new StringBuilder(), new StringBuilder())) {
      LOG.severe("Launching Aurora controller failed");
      return false;
    }
    return true;
  }

  protected boolean verifyState(boolean activate,
                                ExecutionEnvironment.ExecutionState executionState,
                                TopologyAPI.Topology topology,
                                TopologyMaster.TMasterLocation tMasterLocation) {
    // Verify if current state change can be performed.
    if (tMasterLocation == null || executionState == null || topology == null) {
      LOG.severe("Failed to read state. Check if topology is running");
      return false;
    }
    if (!executionState.getRole().equals(role)) {
      LOG.severe("Role not valid. Expected role: "
          + executionState.getRole());
      return false;
    }
    if (activate) {
      if (!topology.getState().equals(TopologyAPI.TopologyState.PAUSED)) {
        LOG.info("Topology not deactivated. State: " + topology.getState());
        return false;
      }
    } else {
      if (!topology.getState().equals(TopologyAPI.TopologyState.RUNNING)) {
        LOG.info("Topology not active. State: " + topology.getState());
        return false;
      }
    }
    return true;
  }

  protected boolean controlTopology(boolean activate) {
    TopologyAPI.Topology topology = null;
    ExecutionEnvironment.ExecutionState executionState = null;
    TopologyMaster.TMasterLocation tMasterLocation = null;
    try {
      SchedulerStateManagerAdaptor manager = context.getStateManagerAdaptor();
      topology = manager.getTopology().get();
      executionState = manager.getExecutionState().get();
      tMasterLocation = manager.getTMasterLocation().get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.log(Level.SEVERE, "Fetch topology or execution state failed." +
          " Confirm that topology is running", e);
      return false;
    }

    if (!verifyState(activate, executionState, topology, tMasterLocation)) {
      LOG.severe("Check topology is running and you have permission to change it's state");
    }

    String controlJobName = "heron-controller-" + topology.getName();
    Map<String, String> args = new HashMap<>();
    args.put("HERON_CONTROLLER_BINARY", "heron-controller");
    args.put("TMASTER_HOSTNAME", tMasterLocation.getHost());
    args.put("TMASTER_CONTROLLER_PORT", "" + tMasterLocation.getControllerPort());
    args.put("TOPOLOGY_ID", topology.getId());
    args.put("JOB_NAME", controlJobName);
    args.put("DC", dc);
    args.put("RUN_ROLE", role);
    args.put("HERON_PACKAGE",
        context.getProperty(Constants.HERON_RELEASE_TAG, "heron-core-package"));
    args.put("RELEASE_ROLE",
        context.getProperty(Constants.HERON_RELEASE_USER_NAME, "heron"));
    args.put("VERSION",
        context.getProperty(Constants.HERON_RELEASE_VERSION, "live"));
    if (activate) {
      args.put("CONTROLLERCMD", "/activate");
    } else {
      args.put("CONTROLLERCMD", "/deactivate");
    }

    return launchAuroraJob(args);
  }

  protected boolean verifyControllerSuccess(boolean activate) {
    // Verify controller succeeded.
    TopologyAPI.Topology topology = null;
    try {
      SchedulerStateManagerAdaptor manager = context.getStateManagerAdaptor();
      topology = manager.getTopology().get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.log(Level.SEVERE, "Fetch topology failed." +
          " Confirm that topology is running", e);
      return false;
    }
    if (activate) {
      LOG.info("Topology End state: " + topology.getState());
      return topology.getState().equals(TopologyAPI.TopologyState.RUNNING);
    } else {
      LOG.info("Topology End state: " + topology.getState());
      return topology.getState().equals(TopologyAPI.TopologyState.PAUSED);
    }
  }

  @Override
  public void initialize(RuntimeManagerContext context) {
    this.context = context;
    this.topologyName = context.getTopologyName();
    this.dc = this.context.getPropertyWithException(Constants.DC);
    this.role = this.context.getPropertyWithException(Constants.ROLE);
    this.environ = this.context.getPropertyWithException(Constants.ENVIRON);
  }

  @Override
  public void close() {
  }

  @Override
  public boolean prepareRestart(int containerIndex) {
    return true;
  }

  @Override
  public boolean postRestart(int restartShardId) {
    TopologyAPI.Topology topology = null;
    try {
      topology = context.getStateManagerAdaptor().getTopology().get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.log(Level.SEVERE, "Fetch topology failed. Check if topology is running", e);
    }
    String batchSize = context.getProperty(HERON_BATCH_SIZE,
        (1 + TopologyUtility.getNumContainer(topology)) + "");
    String[] auroraCmd = new String[]{
        "aurora", "job", "restart", "--batch-size", batchSize,
        restartShardId == -1 ? String.format("%s/%s/%s/%s", dc, role, environ, topologyName)
            : String.format("%s/%s/%s/%s/%s", dc, role, environ, topologyName, restartShardId),
        context.isVerbose() ? "--verbose" : ""
    };

    return 0 == ShellUtility.runProcess(context.isVerbose(), auroraCmd, null, null);
  }

  @Override
  public boolean prepareKill() {
    TopologyAPI.Topology topology = null;
    try {
      topology = context.getStateManagerAdaptor().getTopology().get();
    } catch (InterruptedException | ExecutionException e) {
      LOG.log(Level.SEVERE, "Fetch topology failed. Check if topology is running", e);
      return false;
    }
    String batchSize = context.getProperty(HERON_BATCH_SIZE,
        (1 + TopologyUtility.getNumContainer(topology)) + "");

    String[] auroraCmd = {
        "aurora", "job", "killall", "--batch-size", batchSize,
        String.format("%s/%s/%s/%s", dc, role, environ, topologyName),
        context.isVerbose() ? "--verbose" : ""
    };
    if (0 == ShellUtility.runProcess(
        context.isVerbose(), auroraCmd, null, null)) {
      try {
        ExecutionEnvironment.ExecutionState executionState =
            context.getStateManagerAdaptor().getExecutionState().get();
        String packageName = String.format(
            "heron-topology-%s_%s", topologyName, executionState.getReleaseState().getReleaseTag());

        // Remove package from packer (Unset live label).
        // TODO(nbhagat): Unsetting package live is sufficient. Deleting package is not required
        return 0 == ShellUtility.runProcess(context.isVerbose(),
            new String[]{"packer", "unset_live", "--cluster=" + dc, role, packageName},
            null, null);
      } catch (ExecutionException | InterruptedException e) {
        LOG.log(Level.SEVERE, "Successfully killed aurora job but failed to clear package.", e);
        return false;
      }
    } else {
      LOG.severe("Failed to kill topology.");
      return false;
    }
  }

  @Override
  public boolean postKill() {
    return true;
  }

  @Override
  public boolean prepareDeactivate() {
    writeAuroraFile();
    return controlTopology(false) && verifyControllerSuccess(false);
  }

  @Override
  public boolean postDeactivate() {
    return true;
  }

  @Override
  public boolean prepareActivate() {
    writeAuroraFile();
    return controlTopology(true) && verifyControllerSuccess(true);
  }

  @Override
  public boolean postActivate() {
    return true;
  }
}
