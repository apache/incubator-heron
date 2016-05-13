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

package com.twitter.heron.scheduler.hpc;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.spi.common.ShellUtils;

public class HPCController {
  private static final Logger LOG = Logger.getLogger(HPCController.class.getName());

  private final boolean isVerbose;

  HPCController(boolean isVerbose) {
    this.isVerbose = isVerbose;
  }

  // Create an hpc job
  public boolean createJob(String hpcScript, String heronExec,
                           List<String> commandArgs, String topologyWorkingDirectory,
                           int containers) {
    String nTasks = "--ntasks=" + containers;
    List<String> hpcCmd = new ArrayList<>(Arrays.asList("sbatch", "-N",
        Integer.toString(containers), nTasks, hpcScript, heronExec));

    for (int i = 0; i < commandArgs.size(); i++) {
      String arg = commandArgs.get(i);
      if (arg == null || arg.trim().equals("")) {
        arg = "\"\"";
      }
    }

    hpcCmd.addAll(commandArgs);
    String command = "";
    for (String cmd : hpcCmd) {
      command += cmd + " ";
    }
    LOG.info("Executing job [" + topologyWorkingDirectory + "]: " + command);
    StringBuilder stdout = new StringBuilder();
    StringBuilder stderr = new StringBuilder();
    ShellUtils.runSyncProcess(
        true, false, hpcCmd.toArray(new String[0]), stdout, stderr,
        new File(topologyWorkingDirectory));
    LOG.info("Stdout for HPC script: " + stdout);
    LOG.info("Stderror for HPC script: " + stderr);
    return true;
  }

  // Kill an HPC job
  public boolean killJob(String jobIdFile) {
    List<String> jobIdFileContent = readFromFile(jobIdFile);
    if (jobIdFileContent.size() > 0) {
      List<String> hpcCmd = new ArrayList<>(Arrays.asList("scancel", jobIdFileContent.get(0)));
      return 0 == ShellUtils.runProcess(
          isVerbose, hpcCmd.toArray(new String[0]), new StringBuilder(), new StringBuilder());
    } else {
      LOG.log(Level.SEVERE, "Failed to read the HPC Job id from file:" + jobIdFile);
      return false;
    }
  }

  /**
   * Read the data from a text file
   * For now lets keep this util function here. We need to move it to a util location
   */
  public static List<String> readFromFile(String filename) {
    Path path = new File(filename).toPath();
    List<String> result = new ArrayList<>();
    try {
      List<String> tempResult = Files.readAllLines(path);
      if (tempResult != null) {
        result.addAll(tempResult);
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to read from file. ", e);
    }
    return result;
  }
}
