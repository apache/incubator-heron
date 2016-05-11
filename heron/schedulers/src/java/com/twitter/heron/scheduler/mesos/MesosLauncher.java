package com.twitter.heron.scheduler.mesos;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.core.base.FileUtility;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.scheduler.api.Constants;
import com.twitter.heron.scheduler.api.ILauncher;
import com.twitter.heron.scheduler.api.PackingPlan;
import com.twitter.heron.scheduler.api.SchedulerStateManagerAdaptor;
import com.twitter.heron.scheduler.api.context.LaunchContext;
import com.twitter.heron.scheduler.service.SubmitterMain;
import com.twitter.heron.scheduler.util.NetworkUtility;
import com.twitter.heron.scheduler.util.TopologyUtility;
import com.twitter.heron.state.FileSystemStateManager;

public class MesosLauncher implements ILauncher {
  private static final Logger LOG = Logger.getLogger(MesosLauncher.class.getName());

  private TopologyAPI.Topology topology;
  private LaunchContext context;
  private String dc;
  private String environ;
  private String role;
  private SchedulerStateManagerAdaptor stateManager;

  private static final ObjectMapper mapper = new ObjectMapper();

  @Override
  public void initialize(LaunchContext context) {
    this.topology = context.getTopology();
    this.context = context;
    dc = context.getProperty(Constants.DC);
    environ = context.getProperty(Constants.ENVIRON);
    role = context.getProperty(Constants.ROLE);
  }

  @Override
  public boolean prepareLaunch(PackingPlan packing) {
    LOG.info("Checking whether the topology has been launched already!");

    if (NetworkUtility.awaitResult(context.getStateManagerAdaptor().isTopologyRunning(), 1000, TimeUnit.MILLISECONDS)) {
      LOG.severe("Topology has been running: " + topology.getName());
      return false;
    }
    return true;
  }

  @Override
  public boolean launchTopology(PackingPlan packing) {
    Map<String, String> configOverrides = new HashMap<>();

    // TODO(mfu): Make all these configurable
    configOverrides.put("CLASSPATH", TopologyUtility.makeClasspath(topology));
    configOverrides.put("COMPONENT_JVM_OPTS_IN_BASE64",
        safeEncodeB64(TopologyUtility.getComponentJvmOptions(topology)));
    configOverrides.put("COMPONENT_RAMMAP",
        TopologyUtility.formatRamMap(TopologyUtility.getComponentRamMap(topology)));
    configOverrides.put("DC", dc);
    configOverrides.put("ENVIRON", environ);
    configOverrides.put("HERON_INTERNALS_CONFIG_FILENAME",
        FileUtility.getBaseName(SubmitterMain.getHeronInternalsConfigFile()));
    configOverrides.put("HERON_JAVA_HOME",
        context.getProperty("heron.java.home.path", "/usr/lib/jvm/java-1.8.0-twitter"));
    configOverrides.put("INSTANCE_DISTRIBUTION", TopologyUtility.packingToString(packing));
    configOverrides.put("INSTANCE_JVM_OPTS_IN_BASE64",
        safeEncodeB64(TopologyUtility.getInstanceJvmOptions(topology)));
    configOverrides.put("ISPRODUCTION", "" + ("prod".equals(environ)));
    configOverrides.put("JOB_NAME", topology.getName());
    configOverrides.put("LOG_DIR", context.getProperty("heron.logging.directory", "log-files"));
    configOverrides.put("METRICS_MGR_CLASSPATH", "heron-metricsmgr.jar:metrics-mgr-classpath/*");
    configOverrides.put("NUM_SHARDS", "" + (1 + TopologyUtility.getNumContainer(topology)));
    configOverrides.put("PKG_TYPE", (FileUtility.isOriginalPackageJar(
        FileUtility.getBaseName(SubmitterMain.getOriginalPackageFile())) ? "jar" : "tar"));
    configOverrides.put("STMGR_BINARY", "heron-stmgr");
    configOverrides.put("TMASTER_BINARY", "heron-tmaster");
    configOverrides.put("HERON_SHELL_BINARY", "heron-shell");
    configOverrides.put("TOPOLOGY_DEFN", topology.getName() + ".defn");
    configOverrides.put("TOPOLOGY_ID", topology.getId());
    configOverrides.put("TOPOLOGY_JAR_FILE", FileUtility.getBaseName(SubmitterMain.getOriginalPackageFile()));
    configOverrides.put("TOPOLOGY_NAME", topology.getName());
    configOverrides.put("ZK_NODE", context.getProperty(Constants.ZK_CONNECTION_STRING));
    configOverrides.put("ZK_ROOT", context.getProperty(FileSystemStateManager.ROOT_ADDRESS));
    configOverrides.put(Constants.HERON_CORE_RELEASE_URI,
        String.format("%s", getHeronCoreHdfsPath()));
    configOverrides.put(Constants.TOPOLOGY_PKG_URI,
        String.format("%s", getTopologyHdfsPath()));
    configOverrides.put(MesosConfig.MESOS_MASTER_URI_PREFIX, getMesosMasterUri());
    configOverrides.put(
        Constants.TOPOLOGY_DEFINITION_FILE, topology.getName() + ".defn");
    configOverrides.put(Constants.ROLE, role);

    // Also pass config from Launcher to scheduler.
    for (Map.Entry<Object, Object> entry : context.getConfig().entrySet()) {
      String key = (String) entry.getKey();
      if (!configOverrides.containsKey(key)) {
        configOverrides.put(key, (String) entry.getValue());
      }
    }

    // Executor properties
    LOG.info("Launching topology in mesos");

    StringBuilder overrideBuilder = new StringBuilder();
    for (String key : configOverrides.keySet()) {
      overrideBuilder.append(
          String.format("%s=%s ", key, wrapInQuotes(configOverrides.get(key))));
    }

    overrideBuilder.deleteCharAt(overrideBuilder.length() - 1);

    // Encode the config override into Base64
    String jobDefInJSON = getJobInJSON(safeEncodeB64WWrap(overrideBuilder.toString()));

    String endpoint = String.format("%s/%s",
        context.getPropertyWithException(MesosConfig.HERON_MESOS_FRAMEWORK_ENDPOINT),
        "submit");

    LOG.info("Sending submit request to HSS: " + endpoint);

    HttpURLConnection connection;
    try {
      connection = NetworkUtility.getConnection(endpoint);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to connect to endpoint: " + endpoint);

      return false;
    }

    if (!NetworkUtility.sendHttpPostRequest(connection, jobDefInJSON.getBytes())) {
      LOG.severe("Failed to send http request");
      connection.disconnect();

      return false;
    }

    try {
      if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
        LOG.severe("Response code is not ok: " + connection.getResponseCode());

        return false;
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to get response code", e);
    } finally {
      connection.disconnect();
    }

    return true;
  }

  @Override
  public boolean postLaunch(PackingPlan packing) {
    return true;
  }

  @Override
  public void undo() {

  }

  @Override
  public ExecutionEnvironment.ExecutionState updateExecutionState(
      ExecutionEnvironment.ExecutionState executionState) {
    ExecutionEnvironment.ExecutionState.Builder builder =
        ExecutionEnvironment.ExecutionState.newBuilder().mergeFrom(executionState);

    builder.setDc(dc).setRole(role).setEnviron(environ);

    // Set the HeronReleaseState
    ExecutionEnvironment.HeronReleaseState.Builder releaseBuilder =
        ExecutionEnvironment.HeronReleaseState.newBuilder();
    releaseBuilder.setReleaseUsername(context.getPropertyWithException(Constants.HERON_RELEASE_PACKAGE_ROLE));
    releaseBuilder.setReleaseTag(context.getPropertyWithException(Constants.HERON_RELEASE_PACKAGE_NAME));
    releaseBuilder.setReleaseVersion(context.getPropertyWithException(Constants.HERON_RELEASE_PACKAGE_VERSION));
    releaseBuilder.setUploaderVersion(context.getProperty(Constants.HERON_UPLOADER_VERSION, "live"));

    builder.setReleaseState(releaseBuilder);

    if (builder.isInitialized()) {
      return builder.build();
    } else {
      throw new RuntimeException("Failed to create execution state");
    }
  }

  /**
   * ------------------------------------------------
   * Private methods
   * ------------------------------------------------
   */
  private String getJobInJSON(String configOverride) {
    Map<String, Object> jobDef = new HashMap<>();

    String topologyName = topology.getName();

    String topologyPath = getTopologyHdfsPath();

    String heronCoreReleasePath = getHeronCoreHdfsPath();

    jobDef.put("name", topology.getName() + "-framework");

    jobDef.put("command",
        String.format("%s; %s",
            getSetupCommand(topologyPath, heronCoreReleasePath),
            getRunTopologySchedulerCommand(configOverride)));

    jobDef.put("description", "Scheduler for topology: " + topologyName);

    // TODO(mfu): Make it configurable
    jobDef.put("retries", "" + Integer.MAX_VALUE);
    jobDef.put("owner", role);
    jobDef.put("runAsUser", role);
    jobDef.put("cpu", "1");
    jobDef.put("disk", "1000");
    jobDef.put("mem", "1000");
    jobDef.put("shell", "true");

    List<String> uris = new ArrayList<>();
    uris.add(topologyPath);
    uris.add(heronCoreReleasePath);
    jobDef.put("uris", uris);

    String result = "";
    try {
      result = mapper.writeValueAsString(jobDef);
    } catch (JsonProcessingException e) {
      LOG.log(Level.SEVERE, "Could not convert map to JSONString: " + jobDef.toString(), e);
    }

    return result;
  }

  private String getSetupCommand(String topologyPath,
                                 String heronCoreReleasePath) {
    String topologyFile = extractFilenameFromUri(topologyPath);
    String heronCoreReleaseFile = extractFilenameFromUri(heronCoreReleasePath);

    String untarPackage =
        String.format("ls -1 | grep -v std | xargs -I{} tar xvf {} && rm %s %s",
            topologyFile, heronCoreReleaseFile);

    String freePortWriter =
        String.format("printf '%%s\\n%%s\\n%%s\\n%%s\\n%%s\\n' %s:%s %s:%s %s:%s %s:%s %s:%s |  tee additional_config.conf",
            MesosConfig.TMASTER_MAIN_PORT, "{{task.ports[TMASTER_MAIN_PORT]}}",
            MesosConfig.TMASTER_STAT_PORT, "{{task.ports[TMASTER_STAT_PORT]}}",
            MesosConfig.TMASTER_CONTROLLER_PORT, "{{task.ports[TMASTER_CONTROLLER_PORT]}}",
            MesosConfig.TMASTER_SHELL_PORT, "{{task.ports[TMASTER_SHELL_PORT]}}",
            MesosConfig.TMASTER_METRICSMGR_PORT, "{{task.ports[TMASTER_METRICSMGR_PORT]}}");

    return String.format("%s; %s", untarPackage, freePortWriter);
  }

  private String getRunTopologySchedulerCommand(String configOverride) {
    String topologyName = topology.getName();
    LOG.info("HSS received a submit request for topology: " + topologyName);

    // TODO(mfu): Make this configurable
    StringBuilder sb = new StringBuilder();
    sb.append(this.context.getPropertyWithException("heron.java.home.path") + "/bin/java");
    sb.append(" ");
    sb.append("-cp " + "heron-scheduler.jar");
    sb.append(" ");
    sb.append("-Djava.library.path=.");
    sb.append(" ");

    sb.append("com.twitter.heron.scheduler.service.SchedulerMain ");
    sb.append(topologyName + " ");
    sb.append("com.twitter.heron.scheduler.mesos.MesosScheduler" + " ");
    sb.append("com.twitter.heron.scheduler.util.DefaultConfigLoader" + " ");
    sb.append(configOverride + " ");
    sb.append("{{task.ports[SCHEDULER_SERVER_PORT]}}" + " ");
    sb.append("additional_config.conf");

    String startCommand = sb.toString();

    LOG.info("Topology Scheduler start command: " + startCommand);
    return startCommand;
  }

  private static String extractFilenameFromUri(String url) {
    return url.substring(url.lastIndexOf('/') + 1, url.length());
  }


  private String getMesosMasterUri() {
    String key = String.format("%s.%s.%s", MesosConfig.MESOS_MASTER_URI_PREFIX, dc, environ);
    return context.getPropertyWithException(key);
  }

  private String getHeronCoreHdfsPath() {
    return context.getPropertyWithException(Constants.HERON_CORE_RELEASE_URI);
  }

  private String getTopologyHdfsPath() {
    return context.getPropertyWithException(Constants.TOPOLOGY_PKG_URI);
  }

  private String safeEncodeB64WWrap(String toEncode) {
    return wrapInQuotes(safeEncodeB64(toEncode));
  }

  private String safeEncodeB64(String toEncode) {
    String encoded = DatatypeConverter.printBase64Binary(
            toEncode.getBytes(Charset.forName("UTF-8")));

    return encoded.replace("=", MesosConfig.BASE64_EQUALS);
  }

  private String wrapInQuotes(String toWrap) {
    return String.format("\"%s\"", toWrap);
  }
}
