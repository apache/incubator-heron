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

package org.apache.heron.scheduler.kubernetes;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.heron.scheduler.utils.SchedulerUtils.ExecutorPort;

public final class KubernetesConstants {
  private KubernetesConstants() {
  }

  public static final String MEMORY = "memory";
  public static final String CPU = "cpu";

  // container env constants
  public static final String ENV_HOST = "HOST";
  public static final String POD_IP = "status.podIP";
  public static final String ENV_POD_NAME = "POD_NAME";
  public static final String POD_NAME = "metadata.name";

  public static final String DEFAULT_NAMESPACE = "default";

  // https://kubernetes.io/docs/concepts/workloads/controllers/garbage-collection/
  public static final String DELETE_OPTIONS_PROPAGATION_POLICY = "Foreground";

  // metadata labels
  public static final String LABEL_APP = "app";
  public static final String LABEL_APP_VALUE = "heron";
  public static final String LABEL_TOPOLOGY = "topology";

  // prometheus annotation keys
  public static final String ANNOTATION_PROMETHEUS_SCRAPE = "prometheus.io/scrape";
  public static final String ANNOTATION_PROMETHEUS_PORT = "prometheus.io/port";
  public static final String PROMETHEUS_PORT = "8080";


  public static final int MASTER_PORT = 6001;
  public static final int TMASTER_CONTROLLER_PORT = 6002;
  public static final int TMASTER_STATS_PORT = 6003;
  public static final int SHELL_PORT = 6004;
  public static final int METRICSMGR_PORT = 6005;
  public static final int SCHEDULER_PORT = 6006;
  public static final int METRICS_CACHE_MASTER_PORT = 6007;
  public static final int METRICS_CACHE_STATS_PORT = 6008;
  public static final int CHECKPOINT_MGR_PORT = 6009;
  // port number the start with when more than one port needed for remote debugging
  public static final int JVM_REMOTE_DEBUGGER_PORT = 6010;
  public static final String JVM_REMOTE_DEBUGGER_PORT_NAME = "remote-debugger";

  public static final Map<ExecutorPort, Integer> EXECUTOR_PORTS = new HashMap<>();
  static {
    EXECUTOR_PORTS.put(ExecutorPort.MASTER_PORT, MASTER_PORT);
    EXECUTOR_PORTS.put(ExecutorPort.TMASTER_CONTROLLER_PORT, TMASTER_CONTROLLER_PORT);
    EXECUTOR_PORTS.put(ExecutorPort.TMASTER_STATS_PORT, TMASTER_STATS_PORT);
    EXECUTOR_PORTS.put(ExecutorPort.SHELL_PORT, SHELL_PORT);
    EXECUTOR_PORTS.put(ExecutorPort.METRICS_MANAGER_PORT, METRICSMGR_PORT);
    EXECUTOR_PORTS.put(ExecutorPort.SCHEDULER_PORT, SCHEDULER_PORT);
    EXECUTOR_PORTS.put(ExecutorPort.METRICS_CACHE_MASTER_PORT, METRICS_CACHE_MASTER_PORT);
    EXECUTOR_PORTS.put(ExecutorPort.METRICS_CACHE_STATS_PORT, METRICS_CACHE_STATS_PORT);
    EXECUTOR_PORTS.put(ExecutorPort.CHECKPOINT_MANAGER_PORT, CHECKPOINT_MGR_PORT);
  }

  public static final String JOB_LINK =
      "/api/v1/namespaces/kube-system/services/kubernetes-dashboard/proxy/#/pod";


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

  static final List<String> TOLERATIONS = Collections.unmodifiableList(
      Arrays.asList(
          "node.kubernetes.io/not-ready",
          "node.alpha.kubernetes.io/notReady",
          "node.alpha.kubernetes.io/unreachable"
      )
  );
}
