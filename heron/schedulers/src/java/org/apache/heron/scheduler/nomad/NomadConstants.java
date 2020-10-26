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

package org.apache.heron.scheduler.nomad;

import java.util.HashMap;
import java.util.Map;

import org.apache.heron.scheduler.utils.SchedulerUtils;


public final class NomadConstants {
  private NomadConstants() {
  }

  public enum NomadDriver {
    RAW_EXEC("raw_exec"),
    DOCKER("docker");

    private String name;
    NomadDriver(String name) {
      this.name = name;
    }

    public String getName() {
      return name;
    }
  }

  public static final String NOMAD_TOPOLOGY_ID = "topology.id";
  public static final String NOMAD_TOPOLOGY_NAME = "topology.name";
  public static final String NOMAD_TOPOLOGY_CONTAINER_INDEX = "container.index";

  public static final String JOB_LINK = "/ui/jobs";
  public static final String HOST = "HOST";
  public static final String NETWORK_MODE = "network_mode";

  public static final String NOMAD_TASK_COMMAND = "command";
  public static final String NOMAD_TASK_COMMAND_ARGS = "args";
  public static final String NOMAD_IMAGE = "image";

  public static final String NOMAD_DEFAULT_DATACENTER = "dc1";
  public static final String SHELL_CMD = "/bin/sh";
  public static final String NOMAD_HERON_SCRIPT_NAME = "run_heron_executor.sh";
  public static final String NOMAD_SERVICE_CHECK_TYPE = "tcp";

  public static final String HERON_NOMAD_WORKING_DIR = "HERON_NOMAD_WORKING_DIR";
  public static final String HERON_USE_CORE_PACKAGE_URI = "HERON_USE_CORE_PACKAGE_URI";
  public static final String HERON_CORE_PACKAGE_DIR = "HERON_CORE_PACKAGE_DIR";
  public static final String HERON_CORE_PACKAGE_URI = "HERON_CORE_PACKAGE_URI";
  public static final String HERON_TOPOLOGY_DOWNLOAD_CMD = "HERON_TOPOLOGY_DOWNLOAD_CMD";
  public static final String HERON_EXECUTOR_CMD = "HERON_EXECUTOR_CMD";
  //Ports
  public static final String SERVER_PORT = String.format("${NOMAD_PORT_%s}",
      SchedulerUtils.ExecutorPort.SERVER_PORT.getName());
  public static final String TMANAGER_CONTROLLER_PORT = String.format("${NOMAD_PORT_%s}",
      SchedulerUtils.ExecutorPort.TMANAGER_CONTROLLER_PORT.getName()
          .replace("-", "_"));
  public static final String TMANAGER_STATS_PORT = String.format("${NOMAD_PORT_%s}",
      SchedulerUtils.ExecutorPort.TMANAGER_STATS_PORT.getName()
          .replace("-", "_"));
  public static final String SHELL_PORT = String.format("${NOMAD_PORT_%s}",
      SchedulerUtils.ExecutorPort.SHELL_PORT.getName()
          .replace("-", "_"));
  public static final String METRICS_MANAGER_PORT = String.format("${NOMAD_PORT_%s}",
      SchedulerUtils.ExecutorPort.METRICS_MANAGER_PORT.getName()
          .replace("-", "_"));
  public static final String SCHEDULER_PORT = String.format("${NOMAD_PORT_%s}",
      SchedulerUtils.ExecutorPort.SCHEDULER_PORT.getName()
          .replace("-", "_"));
  public static final String METRICS_CACHE_SERVER_PORT = String.format("${NOMAD_PORT_%s}",
      SchedulerUtils.ExecutorPort.METRICS_CACHE_SERVER_PORT.getName()
          .replace("-", "_"));
  public static final String METRICS_CACHE_STATS_PORT = String.format("${NOMAD_PORT_%s}",
      SchedulerUtils.ExecutorPort.METRICS_CACHE_STATS_PORT.getName()
          .replace("-", "_"));
  public static final String CHECKPOINT_MANAGER_PORT = String.format("${NOMAD_PORT_%s}",
      SchedulerUtils.ExecutorPort.CHECKPOINT_MANAGER_PORT.getName()
          .replace("-", "_"));
  // port number the start with when more than one port needed for remote debugging
  public static final String JVM_REMOTE_DEBUGGER_PORT = String.format("${NOMAD_PORT_%s}",
      SchedulerUtils.ExecutorPort.JVM_REMOTE_DEBUGGER_PORTS.getName());
  // port for metrics webserver (AbstractWebSink)
  public static final String METRICS_PORT = "metrics_port";
  public static final String METRICS_PORT_FILE = "METRICS_PORT_FILE";
  public static final Map<SchedulerUtils.ExecutorPort, String> EXECUTOR_PORTS = new HashMap<>();

  static {
    EXECUTOR_PORTS.put(SchedulerUtils.ExecutorPort.SERVER_PORT, SERVER_PORT);
    EXECUTOR_PORTS.put(SchedulerUtils.ExecutorPort.TMANAGER_CONTROLLER_PORT,
        TMANAGER_CONTROLLER_PORT);
    EXECUTOR_PORTS.put(SchedulerUtils.ExecutorPort.TMANAGER_STATS_PORT, TMANAGER_STATS_PORT);
    EXECUTOR_PORTS.put(SchedulerUtils.ExecutorPort.SHELL_PORT, SHELL_PORT);
    EXECUTOR_PORTS.put(SchedulerUtils.ExecutorPort.METRICS_MANAGER_PORT, METRICS_MANAGER_PORT);
    EXECUTOR_PORTS.put(SchedulerUtils.ExecutorPort.SCHEDULER_PORT, SCHEDULER_PORT);
    EXECUTOR_PORTS.put(SchedulerUtils.ExecutorPort.METRICS_CACHE_SERVER_PORT,
        METRICS_CACHE_SERVER_PORT);
    EXECUTOR_PORTS.put(SchedulerUtils.ExecutorPort.METRICS_CACHE_STATS_PORT,
        METRICS_CACHE_STATS_PORT);
    EXECUTOR_PORTS.put(SchedulerUtils.ExecutorPort.CHECKPOINT_MANAGER_PORT,
        CHECKPOINT_MANAGER_PORT);
  }
}
