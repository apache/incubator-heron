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

  /**
   * Create a hpc job. Use the slurm scheduler's sbatch command to submit the job.
   * sbatch allocates the nodes and runs the script specified by hpcScript.
   * This script runs the heron executor on each of the nodes allocated.
   *
   * @param hpcScript hpc bash script to execute
   * @param heronExec the heron executable
   * @param commandArgs arguments to the heron executor
   * @param topologyWorkingDirectory working directory
   * @param containers number of containers required to run the topology
   * @param partition the queue to submit the job
   * @return true if the job creation is successful
   */
  public boolean createJob(String hpcScript, String heronExec,
                           String[] commandArgs, String topologyWorkingDirectory,
                           long containers, String partition) {
    // get the command to run the job on HPC cluster
    List<String> hpcCmd = hpcCommand(hpcScript, heronExec, containers, partition);

    // change the empty strings of command args to "", because batch
    // doesn't recognize space as an arguments
    List<String> transformedArgs = new ArrayList<>();
    for (int i = 0; i < commandArgs.length; i++) {
      String arg = commandArgs[i];
      if (arg == null || arg.trim().equals("")) {
        transformedArgs.add("\"\"");
      } else {
        transformedArgs.add(arg);
      }
    }

    // add the args to the command
    hpcCmd.addAll(transformedArgs);
    String[] hpcCmdArray = hpcCmd.toArray(new String[0]);
    LOG.log(Level.INFO, "Executing job [" + topologyWorkingDirectory + "]: "
        + Arrays.toString(hpcCmdArray));
    StringBuilder stdout = new StringBuilder();
    StringBuilder stderr = new StringBuilder();
    boolean ret = runProcess(topologyWorkingDirectory, hpcCmdArray, stdout, stderr);
    LOG.log(Level.FINE, "Stdout for HPC script: " + stdout);
    LOG.log(Level.FINE, "Stderror for HPC script: " + stderr);
    return ret;
  }

  /**
   * Construct the HPC Command
   * @param hpcScript hpcscript name
   * @param heronExec heron executable name
   * @param containers number of containers
   * @param partition the partition to submit the job
   * @return list with the command
   */
  private List<String> hpcCommand(String hpcScript, String heronExec,
                                     long containers, String partition) {
    String nTasks = "--ntasks=" + containers;
    List<String> hpcCmd;
    if (partition != null) {
      hpcCmd = new ArrayList<>(Arrays.asList("sbatch", "-N",
          Long.toString(containers), nTasks, "-p", partition, hpcScript, heronExec));
    } else {
      hpcCmd = new ArrayList<>(Arrays.asList("sbatch", "-N",
          Long.toString(containers), nTasks, hpcScript, heronExec));
    }
    return hpcCmd;
  }

  /**
   * Create a hpc job. Use the slurm schedule'r sbatch command to submit the job.
   * sbatch allocates the nodes and runs the script specified by hpcScript.
   * This script runs the heron executor on each of the nodes allocated.
   *
   * @param hpcScript hpc bash script to execute
   * @param heronExec the heron executable
   * @param commandArgs arguments to the heron executor
   * @param topologyWorkingDirectory working directory
   * @param containers number of containers required to run the topology
   * @return true if the job creation is successful
   */
  public boolean createJob(String hpcScript, String heronExec,
                           String[] commandArgs, String topologyWorkingDirectory,
                           long containers) {
    return createJob(hpcScript, heronExec, commandArgs,
        topologyWorkingDirectory, containers, null);
  }

  /**
   * This is for unit testing
   */
  protected boolean runProcess(String topologyWorkingDirectory, String[] hpcCmd,
                         StringBuilder stdout, StringBuilder stderr) {
    return 0 == ShellUtils.runSyncProcess(
        isVerbose, false, hpcCmd, stdout, stderr,
        new File(topologyWorkingDirectory));
  }

  /**
   * Cancel the HPC job by reading the jobid from the jobIdFile. Uses scancel
   * command to cancel the job. The file contains a single line with the job id.
   * This file is written by the hpc job script after the job is allocated.
   * @param jobIdFile the jobId file
   * @return true if the job is cancelled successfully
   */
  public boolean killJob(String jobIdFile) {
    List<String> jobIdFileContent = readFromFile(jobIdFile);
    if (jobIdFileContent.size() > 0) {
      String[] hpcCmd = new String[]{"scancel", jobIdFileContent.get(0)};
      return runProcess(null, hpcCmd, new StringBuilder(), new StringBuilder());
    } else {
      LOG.log(Level.SEVERE, "Failed to read the HPC Job id from file:" + jobIdFile);
      return false;
    }
  }

  /**
   * Read all the data from a text file line by line
   * For now lets keep this util function here. We need to move it to a util location
   * @param filename name of the file
   * @return string list containing the lines of the file, if failed to read, return an empty list
   */
  protected List<String> readFromFile(String filename) {
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
