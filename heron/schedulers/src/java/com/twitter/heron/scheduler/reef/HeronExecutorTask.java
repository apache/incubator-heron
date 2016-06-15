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

package com.twitter.heron.scheduler.reef;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.reef.runtime.common.files.REEFFileNames;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.task.Task;

import com.twitter.heron.api.generated.TopologyAPI.Topology;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.Cluster;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.Environ;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.HeronCorePackageName;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.HeronExecutorId;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.PackedPlan;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.Role;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.TopologyJar;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.TopologyName;
import com.twitter.heron.scheduler.reef.HeronConfigurationOptions.TopologyPackageName;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.ShellUtils;
import com.twitter.heron.spi.utils.SchedulerConfig;
import com.twitter.heron.spi.utils.SchedulerUtils;
import com.twitter.heron.spi.utils.TopologyUtils;

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
  private final String packedPlan;

  private REEFFileNames reefFileNames;
  private String localHeronConfDir;

  @Inject
  public HeronExecutorTask(final REEFFileNames fileNames,
                           @Parameter(HeronExecutorId.class) String heronExecutorId,
                           @Parameter(Cluster.class) String cluster,
                           @Parameter(Role.class) String role,
                           @Parameter(TopologyName.class) String topologyName,
                           @Parameter(Environ.class) String env,
                           @Parameter(TopologyPackageName.class) String topologyPackageName,
                           @Parameter(HeronCorePackageName.class) String heronCorePackageName,
                           @Parameter(TopologyJar.class) String topologyJar,
                           @Parameter(PackedPlan.class) String packedPlan) {
    this.heronExecutorId = Integer.valueOf(heronExecutorId);
    this.cluster = cluster;
    this.role = role;
    this.topologyName = topologyName;
    this.topologyPackageName = topologyPackageName;
    this.heronCorePackageName = heronCorePackageName;
    this.env = env;
    this.topologyJar = topologyJar;
    this.packedPlan = packedPlan;

    reefFileNames = fileNames;
    localHeronConfDir = ".";
  }

  @Override
  public byte[] call(byte[] memento) throws Exception {
    String globalFolder = reefFileNames.getGlobalFolder().getPath();

    HeronReefUtils.extractPackageInSandbox(globalFolder, topologyPackageName, localHeronConfDir);
    HeronReefUtils.extractPackageInSandbox(globalFolder, heronCorePackageName, localHeronConfDir);

    LOG.log(Level.INFO, "Preparing evaluator for running executor-id: {0}", heronExecutorId);
    String[] executorCmd = getExecutorCommand();

    final Process regularExecutor = ShellUtils.runASyncProcess(true, executorCmd, new File("."));
    LOG.log(Level.INFO, "Started heron executor-id: {0}", heronExecutorId);
    regularExecutor.waitFor();
    return null;
  }

  String[] getExecutorCommand() {
    String topologyDefFile = getTopologyDefnFile();
    Topology topology = getTopology(topologyDefFile);
    Config config = SchedulerConfig.loadConfig(cluster,
        role,
        env,
        topologyJar,
        topologyDefFile,
        topology);

    List<Integer> freePorts = new ArrayList<>(SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR);
    for (int i = 0; i < SchedulerUtils.PORTS_REQUIRED_FOR_EXECUTOR; i++) {
      freePorts.add(SysUtils.getFreePort());
    }

    Config runtime = Config.newBuilder()
        .put(Keys.instanceDistribution(), packedPlan)
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
}
