//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License
package com.twitter.heron.scheduler.kubernetes;

/**
 * Created by john on 5/20/17.
 */
public final class KubernetesConstants {
  private KubernetesConstants() {

  }

  public static final String ID = "id";
  public static final String COMMAND = "cmd";
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

  public static final String[] PORT_NAMES = new String[]{
      "master", "tmaster-controller", "tmaster-stats", "shell", "metricsmgr", "scheduler",
      "metrics-cache-master", "metrics-cache-stats"};

  public static final String MASTER_PORT = "6001";
  public static final String TMASTER_CONTROLLER_PORT = "6002";
  public static final String TMASTER_STATS_PORT = "6003";
  public static final String SHELL_PORT = "6004";
  public static final String METRICSMGR_PORT = "6005";
  public static final String SCHEDULER_PORT = "6006";
  public static final String METRICS_CACHE_MASTER_PORT = "6007";
  public static final String METRICS_CACHE_STATS_PORT = "6008";

  public static final String[] PORT_LIST = new String[]{
      MASTER_PORT, TMASTER_CONTROLLER_PORT, TMASTER_STATS_PORT,
      SHELL_PORT, METRICSMGR_PORT, SCHEDULER_PORT, METRICS_CACHE_MASTER_PORT,
      METRICS_CACHE_STATS_PORT};

}
