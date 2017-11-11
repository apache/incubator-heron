// Copyright 2017 Twitter. All rights reserved.
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

package com.twitter.heron.scheduler.ecs;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

public final class EcsConstants {
  private EcsConstants() {

  }

  public static final String ID = "id";
  public static final String COMMAND = "command";
  public static final String APP = "app";
  public static final String NAME = "name";
  public static final String ENV = "env";
  public static final String URI = "uri";
  public static final String TOPOLOGY_LABEL = "topology";
  public static final String PORTS = "ports";
  public static final String PORT_NAME = "name";
  public static final String CONTAINERS = "containers";
  public static final String DOCKER_IMAGE = "image";
  public static final String IMAGE_PULL_POLICY = "imagePullPolicy";
  public static final String DOCKER_CONTAINER_PORT = "containerPort";
  public static final String API_VERSION = "apiVersion";
  public static final String API_VERSION_1 = "v1";
  public static final String API_POD = "Pod";
  public static final String API_METADATA = "metadata";
  public static final String API_KIND = "kind";
  public static final String METADATA_LABELS = "labels";
  public static final String METADATA_ANNOTATIONS = "annotations";
  public static final String MEMORY = "memory";
  public static final String CPU = "cpu";
  public static final String REQUESTS = "requests";
  public static final String RESOURCES = "resources";
  public static final String API_SPEC = "spec";
  public static final String HOST = "HOST";
  public static final String FIELD_PATH = "fieldPath";
  public static final String FIELD_REF = "fieldRef";
  public static final String VALUE_FROM = "valueFrom";
  public static final String POD_IP = "status.podIP";

  // prometheus annotation keys
  public static final String ANNOTATION_PROMETHEUS_SCRAPE = "prometheus.io/scrape";
  public static final String ANNOTATION_PROMETHEUS_PORT = "prometheus.io/port";
  public static final String PROMETHEUS_PORT = "8080";

  public static final String[] PORT_NAMES = new String[]{
      "master", "tmaster-ctlr", "tmaster-stats", "shell", "metricsmgr", "scheduler",
      "metrics-cache-m", "metrics-cache-s", "ckptmgr"};

  public static final String MASTER_PORT = "6001";
  public static final String TMASTER_CONTROLLER_PORT = "6002";
  public static final String TMASTER_STATS_PORT = "6003";
  public static final String SHELL_PORT = "6004";
  public static final String METRICSMGR_PORT = "6005";
  public static final String SCHEDULER_PORT = "6006";
  public static final String METRICS_CACHE_MASTER_PORT = "6007";
  public static final String METRICS_CACHE_STATS_PORT = "6008";
  public static final String CHECKPOINT_MGR_PORT = "6009";

  public static final String[] PORT_LIST = new String[]{
      MASTER_PORT, TMASTER_CONTROLLER_PORT, TMASTER_STATS_PORT,
      SHELL_PORT, METRICSMGR_PORT, SCHEDULER_PORT, METRICS_CACHE_MASTER_PORT,
      METRICS_CACHE_STATS_PORT, CHECKPOINT_MGR_PORT };

  public static final String JOB_LINK =
      "/api/v1/proxy/namespaces/kube-system/services/kubernetes-dashboard/#/pod";


  public static final Pattern VALID_POD_NAME_REGEX =
      Pattern.compile("[a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*",
          Pattern.CASE_INSENSITIVE);

  public static final List<String> VALID_IMAGE_PULL_POLICIES = Collections.unmodifiableList(
      Arrays.asList(
          "IfNotPresent",
          "Always",
          "Never"
      )
  );
}
