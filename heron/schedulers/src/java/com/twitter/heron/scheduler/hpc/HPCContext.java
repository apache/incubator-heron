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

import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Misc;

public class HPCContext extends Context {
  public static final String WORKING_DIRECTORY = "heron.scheduler.local.working.directory";
  public static final String HPC_JOB_ID = "heron.scheduler.hpc.job.id";
  public static final String HPC_SHELL_SCRIPT = "heron.scheduler.hpc.shell.script";
  public static final String HPC_PARTITION = "heron.scheduler.hpc.partition";

  public static String workingDirectory(Config config) {
    String workingDirectory = config.getStringValue(HPCKeys.get("WORKING_DIRECTORY"),
        HPCDefaults.get("WORKING_DIRECTORY"));
    return Misc.substitute(config, workingDirectory);
  }

  public static String jobIdFile(Config config) {
    String jobIdFile = config.getStringValue(
        HPCKeys.get("HPC_JOB_ID"), HPCDefaults.get("HPC_JOB_ID"));
    return Misc.substitute(config, jobIdFile);
  }

  public static String hpcShellScript(Config config) {
    String hpcShellScript = config.getStringValue(HPCKeys.get("HPC_SHELL_SCRIPT"),
        HPCDefaults.get("HPC_SHELL_SCRIPT"));
    return Misc.substitute(config, hpcShellScript);
  }
}
