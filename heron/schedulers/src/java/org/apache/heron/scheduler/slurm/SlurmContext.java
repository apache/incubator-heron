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

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;
import org.apache.heron.spi.common.TokenSub;

public class SlurmContext extends Context {
  public static final String WORKING_DIRECTORY = "heron.scheduler.local.working.directory";
  public static final String SLURM_JOB_ID = "heron.scheduler.slurm.job.id";
  public static final String SLURM_SHELL_SCRIPT = "heron.scheduler.slurm.shell.script";

  public static String workingDirectory(Config config) {
    String workingDirectory = config.getStringValue(WORKING_DIRECTORY,
        "${HOME}/.herondata/topologies/${CLUSTER}/${ROLE}/${TOPOLOGY}");
    return TokenSub.substitute(config, workingDirectory);
  }

  public static String jobIdFile(Config config) {
    return config.getStringValue(SLURM_JOB_ID, "slurm-job.pid");
  }

  public static String slurmShellScript(Config config) {
    return config.getStringValue(SLURM_SHELL_SCRIPT, "slurm.sh");
  }
}
