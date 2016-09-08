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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import com.google.common.annotations.VisibleForTesting;

import com.twitter.heron.spi.utils.ShellUtils;

/**
 * This file defines Utils methods used by Aurora
 */
class AuroraController {
  private final String jobSpec;
  private final boolean isVerbose;

  AuroraController(
      String jobName,
      String cluster,
      String role,
      String env,
      boolean isVerbose) {
    this.isVerbose = isVerbose;
    this.jobSpec = String.format("%s/%s/%s/%s", cluster, role, env, jobName);
  }

  // Create an aurora job
  boolean createJob(String auroraFilename, Map<String, String> bindings) {
    List<String> auroraCmd =
        new ArrayList<>(Arrays.asList("aurora", "job", "create", "--wait-until", "RUNNING"));

    for (Map.Entry<String, String> binding : bindings.entrySet()) {
      auroraCmd.add("--bind");
      auroraCmd.add(String.format("%s=%s", binding.getKey(), binding.getValue()));
    }

    auroraCmd.add(jobSpec);
    auroraCmd.add(auroraFilename);

    if (isVerbose) {
      auroraCmd.add("--verbose");
    }

    return runProcess(auroraCmd);
  }

  // Kill an aurora job
  boolean killJob() {
    List<String> auroraCmd = new ArrayList<>(Arrays.asList("aurora", "job", "killall"));
    auroraCmd.add(jobSpec);

    appendAuroraCommandOptions(auroraCmd, isVerbose);

    return runProcess(auroraCmd);
  }

  // Restart an aurora job
  boolean restartJob(int containerId) {
    List<String> auroraCmd = new ArrayList<>(Arrays.asList("aurora", "job", "restart"));
    if (containerId != -1) {
      auroraCmd.add(String.format("%s/%s", jobSpec, Integer.toString(containerId)));
    } else {
      auroraCmd.add(jobSpec);
    }

    appendAuroraCommandOptions(auroraCmd, isVerbose);

    return runProcess(auroraCmd);
  }

  // Utils method for unit tests
  @VisibleForTesting
  boolean runProcess(List<String> auroraCmd) {
    return 0 == ShellUtils.runProcess(
        isVerbose, auroraCmd.toArray(new String[auroraCmd.size()]),
        new StringBuilder(), new StringBuilder());
  }

  // Static method to append verbose and batching options if needed
  private static void appendAuroraCommandOptions(List<String> auroraCmd, boolean isVerbose) {
    // Append verbose if needed
    if (isVerbose) {
      auroraCmd.add("--verbose");
    }

    // Append batch size.
    // Note that we can not use "--no-batching" since "restart" command does not accept it.
    // So we play a small trick here by setting batch size Integer.MAX_VALUE.
    auroraCmd.add("--batch-size");
    auroraCmd.add(Integer.toString(Integer.MAX_VALUE));
  }
}
