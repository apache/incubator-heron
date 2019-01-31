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

package org.apache.heron.scheduler.slurm;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.heron.spi.utils.ShellUtils;

public class SlurmController {
  private static final Logger LOG = Logger.getLogger(SlurmController.class.getName());

  private final boolean isVerbose;

  SlurmController(boolean isVerbose) {
    this.isVerbose = isVerbose;
  }

  /**
   * Create a slurm job. Use the slurm scheduler's sbatch command to submit the job.
   * sbatch allocates the nodes and runs the script specified by slurmScript.
   * This script runs the heron executor on each of the nodes allocated.
   *
   * @param slurmScript slurm bash script to execute
   * @param heronExec the heron executable
   * @param commandArgs arguments to the heron executor
   * @param topologyWorkingDirectory working directory
   * @param containers number of containers required to run the topology
   * @param partition the queue to submit the job
   * @return true if the job creation is successful
   */
  public boolean createJob(String slurmScript, String heronExec,
                           String[] commandArgs, String topologyWorkingDirectory,
                           long containers, String partition) {
    // get the command to run the job on Slurm cluster
    List<String> slurmCmd = slurmCommand(slurmScript, heronExec, containers, partition);

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
    slurmCmd.addAll(transformedArgs);
    String[] slurmCmdArray = slurmCmd.toArray(new String[0]);
    LOG.log(Level.INFO, "Executing job [" + topologyWorkingDirectory + "]:",
        Arrays.toString(slurmCmdArray));
    StringBuilder stderr = new StringBuilder();
    boolean ret = runProcess(topologyWorkingDirectory, slurmCmdArray, stderr);
    return ret;
  }

  /**
   * Construct the SLURM Command
   * @param slurmScript slurm script name
   * @param heronExec heron executable name
   * @param containers number of containers
   * @param partition the partition to submit the job
   * @return list with the command
   */
  private List<String> slurmCommand(String slurmScript, String heronExec,
                                    long containers, String partition) {
    String nTasks = String.format("--ntasks=%d", containers);
    List<String> slurmCmd;
    if (partition != null) {
      slurmCmd = new ArrayList<>(Arrays.asList("sbatch", "-N",
          Long.toString(containers), nTasks, "-p", partition, slurmScript, heronExec));
    } else {
      slurmCmd = new ArrayList<>(Arrays.asList("sbatch", "-N",
          Long.toString(containers), nTasks, slurmScript, heronExec));
    }
    return slurmCmd;
  }

  /**
   * Create a slurm job. Use the slurm schedule'r sbatch command to submit the job.
   * sbatch allocates the nodes and runs the script specified by slurmScript.
   * This script runs the heron executor on each of the nodes allocated.
   *
   * @param slurmScript slurm bash script to execute
   * @param heronExec the heron executable
   * @param commandArgs arguments to the heron executor
   * @param topologyWorkingDirectory working directory
   * @param containers number of containers required to run the topology
   * @return true if the job creation is successful
   */
  public boolean createJob(String slurmScript, String heronExec,
                           String[] commandArgs, String topologyWorkingDirectory,
                           long containers) {
    return createJob(slurmScript, heronExec, commandArgs,
        topologyWorkingDirectory, containers, null);
  }

  /**
   * This is for unit testing
   */
  protected boolean runProcess(String topologyWorkingDirectory, String[] slurmCmd,
                               StringBuilder stderr) {
    File file = topologyWorkingDirectory == null ? null : new File(topologyWorkingDirectory);
    return 0 == ShellUtils.runSyncProcess(true, false, slurmCmd, stderr, file);
  }

  /**
   * Cancel the Slurm job by reading the jobid from the jobIdFile. Uses scancel
   * command to cancel the job. The file contains a single line with the job id.
   * This file is written by the slurm job script after the job is allocated.
   * @param jobIdFile the jobId file
   * @return true if the job is cancelled successfully
   */
  public boolean killJob(String jobIdFile) {
    List<String> jobIdFileContent = readFromFile(jobIdFile);
    if (jobIdFileContent.size() > 0) {
      String[] slurmCmd = new String[]{"scancel", jobIdFileContent.get(0)};
      return runProcess(null, slurmCmd, new StringBuilder());
    } else {
      LOG.log(Level.SEVERE, "Failed to read the Slurm Job id from file: {0}", jobIdFile);
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
