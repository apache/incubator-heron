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

package org.apache.heron.scheduler.marathon;

import java.util.HashMap;
import java.util.Map;

import org.apache.heron.scheduler.utils.SchedulerUtils.ExecutorPort;

public final class MarathonConstants {
  private MarathonConstants() {

  }

  public static final String ID = "id";
  public static final String MARATHON_GROUP_PATH = "/heron/topologies/";
  public static final String COMMAND = "cmd";
  public static final String CPU = "cpus";
  public static final String MEMORY = "mem";
  public static final String DISK = "disk";
  public static final String PORT_DEFINITIONS = "portDefinitions";
  public static final String INSTANCES = "instances";
  public static final String LABELS = "labels";
  public static final String FETCH = "fetch";
  public static final String USER = "user";
  public static final String APPS = "apps";
  public static final String ENVIRONMENT = "environment";
  public static final String URI = "uri";
  public static final String EXECUTABLE = "executable";
  public static final String EXTRACT = "extract";
  public static final String CACHE = "cache";
  public static final String PORT = "port";
  public static final String PROTOCOL = "protocol";
  public static final String PORT_NAME = "name";
  public static final String TCP = "tcp";
  public static final String CONTAINER = "container";
  public static final String CONTAINER_TYPE = "type";
  public static final String DOCKER_IMAGE = "image";
  public static final String DOCKER_NETWORK = "network";
  public static final String DOCKER_PORT_MAPPINGS = "portMappings";
  public static final String DOCKER_CONTAINER_PORT = "containerPort";
  public static final String HOST_PORT = "hostPort";
  public static final String DOCKER_PRIVILEGED = "privileged";
  public static final String DOCKER_FORCE_PULL = "forcePullImage";
  public static final String DOCKER_NETWORK_BRIDGE = "BRIDGE";

  public static final String SERVER_PORT = "$PORT0";
  public static final String TMANAGER_CONTROLLER_PORT = "$PORT1";
  public static final String TMANAGER_STATS_PORT = "$PORT2";
  public static final String SHELL_PORT = "$PORT3";
  public static final String METRICSMGR_PORT = "$PORT4";
  public static final String SCHEDULER_PORT = "$PORT5";
  public static final String METRICS_CACHE_SERVER_PORT = "$PORT6";
  public static final String METRICS_CACHE_STATS_PORT = "$PORT7";
  public static final String CKPTMGR_PORT = "$PORT8";

  public static final Map<ExecutorPort, String> EXECUTOR_PORTS = new HashMap<>();
  static {
    EXECUTOR_PORTS.put(ExecutorPort.SERVER_PORT, SERVER_PORT);
    EXECUTOR_PORTS.put(ExecutorPort.TMANAGER_CONTROLLER_PORT, TMANAGER_CONTROLLER_PORT);
    EXECUTOR_PORTS.put(ExecutorPort.TMANAGER_STATS_PORT, TMANAGER_STATS_PORT);
    EXECUTOR_PORTS.put(ExecutorPort.SHELL_PORT, SHELL_PORT);
    EXECUTOR_PORTS.put(ExecutorPort.METRICS_MANAGER_PORT, METRICSMGR_PORT);
    EXECUTOR_PORTS.put(ExecutorPort.SCHEDULER_PORT, SCHEDULER_PORT);
    EXECUTOR_PORTS.put(ExecutorPort.METRICS_CACHE_SERVER_PORT, METRICS_CACHE_SERVER_PORT);
    EXECUTOR_PORTS.put(ExecutorPort.METRICS_CACHE_STATS_PORT, METRICS_CACHE_STATS_PORT);
    EXECUTOR_PORTS.put(ExecutorPort.CHECKPOINT_MANAGER_PORT, CKPTMGR_PORT);
  }

  public static final String JOB_LINK = "/ui/#/group/%2F";
}
