// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.scheduler.aurora;

import java.nio.charset.Charset;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import javax.xml.bind.DatatypeConverter;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.FileUtils;
import com.twitter.heron.proto.system.ExecutionEnvironment;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.common.PackingPlan;

import com.twitter.heron.spi.scheduler.ILauncher;
import com.twitter.heron.spi.scheduler.context.LaunchContext;

import com.twitter.heron.scheduler.service.SubmitterMain;
import com.twitter.heron.scheduler.twitter.PackerUtility;

import com.twitter.heron.scheduler.util.NetworkUtility;
import com.twitter.heron.scheduler.util.ShellUtility;
import com.twitter.heron.scheduler.util.TopologyUtility;

/**
 * Launch topology on aurora.
 */
public class AuroraLauncher implements ILauncher {
  private static final Logger LOG = Logger.getLogger(AuroraLauncher.class.getName());
  private static final String HERON_AURORA = "heron.aurora";

  //  private DefaultConfigLoader context;
  private LaunchContext context;
  private String cluster;
  private String environ;
  private String role;
  private TopologyAPI.Topology topology;

  @Override
  public void initialize(LaunchContext context) {
    this.context = context;
    this.cluster = context.getPropertyWithException(Constants.CLUSTER);
    this.environ = context.getPropertyWithException(Constants.ENVIRON);
    this.role = context.getPropertyWithException(Constants.ROLE);
    this.topology = context.getTopology();
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
  public void undo() {
    // TODO(nbhagat): Delete topology if has been launched.
  }

  @Override
  public ExecutionEnvironment.ExecutionState updateExecutionState(
      ExecutionEnvironment.ExecutionState executionState) {
    ExecutionEnvironment.ExecutionState.Builder builder =
        ExecutionEnvironment.ExecutionState.newBuilder().mergeFrom(executionState);

    builder.setDc(cluster).setRole(role).setEnviron(environ);

    // Set the HeronReleaseState
    ExecutionEnvironment.HeronReleaseState.Builder releaseBuilder =
        ExecutionEnvironment.HeronReleaseState.newBuilder();
    // TODO(jwu): Update the following constants to proper names.
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

  private String formatJavaOpts(String javaOpts) {
    String javaOptsBase64 = DatatypeConverter.printBase64Binary(javaOpts.getBytes(Charset.forName("UTF-8")));
    return String.format("\"%s\"", javaOptsBase64.replace("=", "&equals;"));
  }

  private String getZkRoot() {
    return context.getPropertyWithException(Constants.ZKROOT);
  }

  private String getZkHostPort() {
    String zkHost = context.getPropertyWithException(Constants.ZKHOST);
    String zkPort = context.getPropertyWithException(Constants.ZKPORT);
    return zkHost + ":" + zkPort;
  }

  private void addCustomBindProperties(Map<String, String> auroraProperties) {
    String keyPrefix = Constants.HERON_AURORA_BIND_PREFIX;

    for(Object key: context.getConfig().keySet()) {
      String strKey = key.toString();
      if (strKey.startsWith(keyPrefix)) {
        String bindKey = strKey.substring(keyPrefix.length());
        String bindValue = context.getConfig().get(key).toString();
        auroraProperties.put(bindKey, bindValue);
      }
    }
  }

  private String getHeronAuroraPath() {
    String configDir = context.getConfig().get(Constants.HERON_CONFIG_PATH).toString();
    return Paths.get(configDir, HERON_AURORA).toString();
  }

  @Override
  public boolean launchTopology(PackingPlan packing) {
    LOG.info("Launching topology in aurora");
    if (packing == null || packing.containers.isEmpty()) {
      LOG.severe("No container requested. Can't schedule");
      return false;
    }
    PackingPlan.Resource containerResource =
        packing.containers.values().iterator().next().resource;
    Map<String, String> auroraProperties = new HashMap<>();

    auroraProperties.put("CLASSPATH", TopologyUtility.makeClasspath(topology));
    auroraProperties.put("COMPONENT_JVM_OPTS_IN_BASE64",
        formatJavaOpts(TopologyUtility.getComponentJvmOptions(topology)));
    auroraProperties.put("COMPONENT_RAMMAP",
        TopologyUtility.formatRamMap(TopologyUtility.getComponentRamMap(topology)));
    auroraProperties.put("CPUS_PER_CONTAINER", containerResource.cpu + "");
    auroraProperties.put("CLUSTER", cluster);
    auroraProperties.put("DISK_PER_CONTAINER", containerResource.disk + "");
    auroraProperties.put("ENVIRON", environ);
    auroraProperties.put("HERON_EXECUTOR_BINARY", "heron-executor");
    auroraProperties.put("HERON_INTERNALS_CONFIG_FILENAME",
        FileUtils.getBaseName(SubmitterMain.getHeronInternalsConfigFile()));
    auroraProperties.put("HERON_JAVA_HOME",
        context.getProperty("heron.java.home.path", ""));
    auroraProperties.put("INSTANCE_DISTRIBUTION", TopologyUtility.packingToString(packing));
    auroraProperties.put("INSTANCE_JVM_OPTS_IN_BASE64",
        formatJavaOpts(TopologyUtility.getInstanceJvmOptions(topology)));
    auroraProperties.put("ISPRODUCTION", "" + "prod".equals(environ));
    auroraProperties.put("JOB_NAME", topology.getName());
    auroraProperties.put("LOG_DIR",
        context.getProperty("heron.logging.directory", "log-files"));
    auroraProperties.put("METRICS_MGR_CLASSPATH", "metrics-mgr-classpath/*");
    auroraProperties.put("NUM_SHARDS", "" + (1 + TopologyUtility.getNumContainer(topology)));
    auroraProperties.put("PKG_TYPE", (FileUtils.isOriginalPackageJar(
        FileUtils.getBaseName(SubmitterMain.getOriginalPackageFile())) ? "jar" : "tar"));
    auroraProperties.put("RAM_PER_CONTAINER", containerResource.ram + "");
    auroraProperties.put("RUN_ROLE", role);
    auroraProperties.put("STMGR_BINARY", "heron-stmgr");
    auroraProperties.put("TMASTER_BINARY", "heron-tmaster");
    auroraProperties.put("SHELL_BINARY", "heron-shell");
    auroraProperties.put("TOPOLOGY_DEFN", topology.getName() + ".defn");
    auroraProperties.put("TOPOLOGY_ID", topology.getId());
    auroraProperties.put("TOPOLOGY_JAR_FILE",
        FileUtils.getBaseName(SubmitterMain.getOriginalPackageFile()));
    auroraProperties.put("TOPOLOGY_NAME", topology.getName());
    auroraProperties.put("TOPOLOGY_PKG",
        PackerUtility.getTopologyPackageName(topology.getName(),
            context.getProperty(Constants.HERON_RELEASE_PACKAGE_NAME, "live")));
    auroraProperties.put("ZK_NODE", getZkHostPort());
    auroraProperties.put("ZK_ROOT", getZkRoot());

    addCustomBindProperties(auroraProperties);

    ArrayList<String> auroraCmd = new ArrayList<>(Arrays.asList(
        "aurora", "job", "create", "--verbose", "--wait-until", "RUNNING"));
    for (String binding : auroraProperties.keySet()) {
      auroraCmd.add("--bind");
      auroraCmd.add(String.format("%s=%s", binding, auroraProperties.get(binding)));
    }
    auroraCmd.add(String.format("%s/%s/%s/%s",
        auroraProperties.get("CLUSTER"),
        auroraProperties.get("RUN_ROLE"),
        auroraProperties.get("ENVIRON"),
        auroraProperties.get("JOB_NAME")));
    auroraCmd.add(getHeronAuroraPath());

    String[] cmdline = auroraCmd.toArray(new String[auroraCmd.size()]);
    LOG.info("cmdline=" + Arrays.toString(cmdline));

    return 0 == ShellUtility.runProcess(true, cmdline,new StringBuilder(), new StringBuilder());
  }

  @Override
  public boolean postLaunch(PackingPlan packing) {
    return true;
  }
}
