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

import com.twitter.heron.spi.common.ShellUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HPCController {
  private static Logger LOG = Logger.getLogger(HPCController.class.getName());

  private final boolean isVerbose;

  HPCController(boolean isVerbose) {
    this.isVerbose = isVerbose;
  }

  // Create an aurora job
  public boolean createJob(String hpcScript, String heronExec, List<String> commandArgs, String topologyWorkingDirectory, int containers) {
    String nTasks = "--ntasks=" + containers;
//    List<String> hpcCmd = new ArrayList<>(Arrays.asList("sbatch", "-N", Integer.toString(containers), nTasks, hpcScript, heronExec));
    List<String> hpcCmd = new ArrayList<>(Arrays.asList("sbatch", hpcScript, heronExec));

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
        true, false, hpcCmd.toArray(new String[0]), stdout, stderr, new File(topologyWorkingDirectory));
    LOG.info("Stdout for HPC script: " + stdout);
    LOG.info("Stderror for HPC script: " + stderr);
    return true;
  }

  // Kill an HPC job
  public boolean killJob(String jobIdFile) {
    String jobId = readFromFile(jobIdFile);
    List<String> hpcCmd = new ArrayList<>(Arrays.asList("scancel", jobId));
    return 0 == ShellUtils.runProcess(
        isVerbose, hpcCmd.toArray(new String[0]), new StringBuilder(), new StringBuilder());
  }


  public static String readFromFile(String filename) {
    Path path = new File(filename).toPath();
    List<String> res;
    try {
      res = Files.readAllLines(path);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to read from file. ", e);
      return null;
    }
    if (res != null && res.size() >= 1) {
      return res.get(0).trim();
    }
    return null;
  }
}
