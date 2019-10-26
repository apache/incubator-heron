/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.scheduler.mesos;

import java.io.File;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Logger;

import org.apache.heron.common.basics.FileUtils;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.scheduler.utils.Runtime;
import org.apache.heron.scheduler.utils.SchedulerUtils;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.packing.PackingPlan;
import org.apache.heron.spi.scheduler.ILauncher;
import org.apache.heron.spi.utils.ShellUtils;

/**
 * Launch a topology to mesos cluster
 */
public class MesosLauncher implements ILauncher {
  private static final Logger LOG = Logger.getLogger(MesosLauncher.class.getName());

  private Config config;
  private Config runtime;

  private String schedulerWorkingDirectory;

  @Override
  public void initialize(Config mConfig, Config mRuntime) {
    this.config = mConfig;
    this.runtime = mRuntime;

    // get the scheduler working directory
    this.schedulerWorkingDirectory = MesosContext.getSchedulerWorkingDirectory(config);
  }

  @Override
  public void close() {

  }

  @Override
  public boolean launch(PackingPlan packing) {
    // setup the scheduler working directory
    // mainly it downloads and extracts the topology package
    if (!setupWorkingDirectory()) {
      LOG.severe("Failed to setup working directory");
      return false;
    }

    String[] schedulerCmd = getSchedulerCommand();

    Process p = startScheduler(schedulerCmd);

    if (p == null) {
      LOG.severe("Failed to start SchedulerMain using: " + Arrays.toString(schedulerCmd));
      return false;
    }

    LOG.info(String.format(
        "For checking the status and logs of the topology, use the working directory %s",
        MesosContext.getSchedulerWorkingDirectory(config)));

    return true;
  }

  protected String[] getSchedulerCommand() {
    List<Integer> freePorts = new ArrayList<>(SchedulerUtils.PORTS_REQUIRED_FOR_SCHEDULER);
    for (int i = 0; i < SchedulerUtils.PORTS_REQUIRED_FOR_SCHEDULER; i++) {
      freePorts.add(SysUtils.getFreePort());
    }

    List<String> commands = new ArrayList<>();

    // The java executable should be "{JAVA_HOME}/bin/java"
    String javaExecutable = String.format("%s/%s", Context.javaHome(config), "bin/java");
    commands.add(javaExecutable);

    // Add the path to load mesos library
    commands.add(String.format("-Djava.library.path=%s",
        MesosContext.getHeronMesosNativeLibraryPath(config)));
    commands.add("-cp");

    // Construct the complete classpath to start scheduler
    String completeSchedulerProcessClassPath = System.getProperty("java.class.path");
    commands.add(completeSchedulerProcessClassPath);
    commands.add("org.apache.heron.scheduler.SchedulerMain");

    // Add default scheduler properties override
    // Add the topology package uri as a String and pass the SchedulerMain as a property
    commands.add(String.format("-%s%s=%s",
        SchedulerUtils.SCHEDULER_COMMAND_LINE_PROPERTIES_OVERRIDE_OPTION,
        Key.TOPOLOGY_PACKAGE_URI.value(),
        Runtime.topologyPackageUri(runtime).toString()));

    String[] commandArgs = schedulerCommandArgs(freePorts);
    commands.addAll(Arrays.asList(commandArgs));

    return commands.toArray(new String[0]);
  }

  ///////////////////////////////////////////////////////////////////////////////
  // Utils methods leveraging unit tests
  ///////////////////////////////////////////////////////////////////////////////
  protected boolean setupWorkingDirectory() {
    if (!FileUtils.isDirectoryExists(schedulerWorkingDirectory)) {
      FileUtils.createDirectory(schedulerWorkingDirectory);
    }

    String topologyPackageURL = String.format("file://%s", Context.topologyPackageFile(config));
    String topologyPackageDestination = Paths.get(
        schedulerWorkingDirectory, "topology.tar.gz").toString();


    return SchedulerUtils.curlAndExtractPackage(
        schedulerWorkingDirectory, topologyPackageURL,
        topologyPackageDestination, true, Context.verbose(config));
  }

  protected String[] schedulerCommandArgs(List<Integer> freePorts) {
    return SchedulerUtils.schedulerCommandArgs(config, runtime, freePorts);
  }

  protected Process startScheduler(String[] schedulerCmd) {
    return ShellUtils.runASyncProcess(Context.verbose(config), schedulerCmd,
        new File(schedulerWorkingDirectory));
  }
}
