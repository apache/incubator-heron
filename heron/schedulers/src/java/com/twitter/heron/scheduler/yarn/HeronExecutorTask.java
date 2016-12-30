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

package com.twitter.heron.scheduler.yarn;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.task.Task;
import org.apache.reef.task.events.CloseEvent;
import org.apache.reef.wake.EventHandler;

import com.twitter.heron.api.generated.TopologyAPI.Topology;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.scheduler.utils.SchedulerConfigUtils;
import com.twitter.heron.scheduler.utils.SchedulerUtils;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.Cluster;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.ComponentRamMap;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.Environ;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.HeronCorePackageName;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.HeronExecutorId;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.Role;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.TopologyJar;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.TopologyName;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.TopologyPackageName;
import com.twitter.heron.scheduler.yarn.HeronConfigurationOptions.VerboseLogMode;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.utils.ShellUtils;
import com.twitter.heron.spi.utils.TopologyUtils;

@Unit
public class HeronExecutorTask implements Task {
  private static final Logger LOG = Logger.getLogger(HeronExecutorTask.class.getName());

  private final String topologyPackageName;
  private final String heronCorePackageName;
  private final int heronExecutorId;
  private final String cluster;
  private final String role;
  private final String topologyName;
  private final String env;
  private final String topologyJar;
  private final String componentRamMap;
  private final boolean verboseMode;

  private REEFFileNames reefFileNames;
  private String localHeronConfDir;

  // Reference to the thread waiting for heron executor to complete
  private volatile Thread processTarget;

  @Inject
  public HeronExecutorTask(final REEFFileNames fileNames,
                           @Parameter(HeronExecutorId.class) int heronExecutorId,
                           @Parameter(Cluster.class) String cluster,
                           @Parameter(Role.class) String role,
                           @Parameter(TopologyName.class) String topologyName,
                           @Parameter(Environ.class) String env,
                           @Parameter(TopologyPackageName.class) String topologyPackageName,
                           @Parameter(HeronCorePackageName.class) String heronCorePackageName,
                           @Parameter(TopologyJar.class) String topologyJar,
                           @Parameter(ComponentRamMap.class) String componentRamMap,
                           @Parameter(VerboseLogMode.class) boolean verboseMode) {
    this.heronExecutorId = heronExecutorId;
    this.cluster = cluster;
    this.role = role;
    this.topologyName = topologyName;
    this.topologyPackageName = topologyPackageName;
    this.heronCorePackageName = heronCorePackageName;
    this.env = env;
    this.topologyJar = topologyJar;
    this.componentRamMap = componentRamMap;
    this.verboseMode = verboseMode;

    reefFileNames = fileNames;
    localHeronConfDir = ".";
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    String globalFolder = reefFileNames.getGlobalFolder().getPath();

    HeronReefUtils.extractPackageInSandbox(globalFolder, topologyPackageName, localHeronConfDir);
    HeronReefUtils.extractPackageInSandbox(globalFolder, heronCorePackageName, localHeronConfDir);

    startExecutor();
    return null;
  }

  public void startExecutor() {
    LOG.log(Level.INFO, "Preparing evaluator for running executor-id: {0}", heronExecutorId);
    String[] executorCmd = getExecutorCommand();

    processTarget = Thread.currentThread();

    // Log the working directory, this will make people fast locate the
    // directory to find the log files
    File workingDirectory = new File(".");
    String cwdPath = workingDirectory.getAbsolutePath();
    LOG.log(Level.INFO, "Working dir: {0}", cwdPath);

    HashMap<String, String> executorEnvironment = getEnvironment(cwdPath);

    final Process regularExecutor = ShellUtils.runASyncProcess(
        true,
        executorCmd,
        workingDirectory,
        executorEnvironment);
    LOG.log(Level.INFO, "Started heron executor-id: {0}", heronExecutorId);
    try {
      regularExecutor.waitFor();
      LOG.log(Level.WARNING, "Heron executor process terminated");
    } catch (InterruptedException e) {
      LOG.log(Level.INFO, "Destroy heron executor-id: {0}", heronExecutorId);
      regularExecutor.destroy();
    }
  }

  HashMap<String, String> getEnvironment(String cwdPath) {
    HashMap<String, String> envs = new HashMap<>();
    envs.put("PEX_ROOT", cwdPath);
    return envs;
  }

  String[] getExecutorCommand() {
    String topologyDefFile = getTopologyDefnFile();
    Topology topology = getTopology(topologyDefFile);
    Config config = SchedulerConfigUtils.loadConfig(cluster,
        role,
        env,
        topologyJar,
        topologyDefFile,
        verboseMode,
        topology);

    List<Integer> freePorts = new ArrayList<>(SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR);
    for (int i = 0; i < SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR; i++) {
      freePorts.add(SysUtils.getFreePort());
    }

    Config runtime = Config.newBuilder()
        .put(Keys.componentRamMap(), componentRamMap)
        .put(Keys.topologyDefinition(), topology)
        .build();

    String[] executorCmd = SchedulerUtils.executorCommand(config,
        runtime,
        heronExecutorId,
        freePorts);

    LOG.info("Executor command line: " + Arrays.toString(executorCmd));
    return executorCmd;
  }

  String getTopologyDefnFile() {
    return TopologyUtils.lookUpTopologyDefnFile(".", topologyName);
  }

  Topology getTopology(String topologyDefFile) {
    return TopologyUtils.getTopology(topologyDefFile);
  }

  /**
   * This class will kill heron executor process when topology or container restart is requested.
   */
  public final class HeronExecutorTaskTerminator implements EventHandler<CloseEvent> {
    @Override
    public void onNext(CloseEvent closeEvent) {
      LOG.log(Level.INFO, "Received request to terminate executor-id: {0}", heronExecutorId);
      processTarget.interrupt();
    }
  }
}
