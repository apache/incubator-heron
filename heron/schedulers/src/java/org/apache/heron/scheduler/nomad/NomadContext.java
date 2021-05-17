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

import java.io.File;
import java.util.logging.Logger;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Context;

public class NomadContext extends Context {
  private static final Logger LOG = Logger.getLogger(NomadContext.class.getName());

  public static final String HERON_NOMAD_SCHEDULER_URI = "heron.nomad.scheduler.uri";

  public static final String HERON_NOMAD_CORE_FREQ_MAPPING = "heron.nomad.core.freq.mapping";

  public static final String HERON_NOMAD_DRIVER = "heron.nomad.driver";

  public static final String HERON_EXECUTOR_DOCKER_IMAGE = "heron.executor.docker.image";

  public static final String HERON_NOMAD_NETWORK_MODE = "heron.nomad.network.mode";

  public static final String HERON_NOMAD_METRICS_SERVICE_REGISTER
      = "heron.nomad.metrics.service.register";

  public static final String HERON_NOMAD_METRICS_SERVICE_CHECK_INTERVAL_SEC
      = "heron.nomad.metrics.service.check.interval.sec";

  public static final String HERON_NOMAD_METRICS_SERVICE_CHECK_TIMEOUT_SEC
      = "heron.nomad.metrics.service.check.timeout.sec";

  public static final String HERON_NOMAD_METRICS_SERVICE_ADDITIONAL_TAGS
      = "heron.nomad.metrics.service.additional.tags";

  public static String workingDirectory(Config config) {
    return config.getStringValue(
        NomadKey.WORKING_DIRECTORY.value(), NomadKey.WORKING_DIRECTORY.getDefaultString());
  }

  public static String getHeronNomadPath(Config config) {
    String filePath = config.getStringValue(NomadKey.JOB_TEMPLATE.value(),
        Context.heronConf(config) + "/" + NomadKey.JOB_TEMPLATE.getDefaultString());
    return new File(filePath).getPath();
  }

  public static String getSchedulerURI(Config config) {
    return config.getStringValue(HERON_NOMAD_SCHEDULER_URI, "http://127.0.0.1:4646");
  }

  public static int getCoreFreqMapping(Config config) {
    return config.getIntegerValue(HERON_NOMAD_CORE_FREQ_MAPPING, 1000);
  }

  public static String getHeronNomadDriver(Config config) {
    return config.getStringValue(HERON_NOMAD_DRIVER, "docker");
  }

  public static String getHeronExecutorDockerImage(Config config) {
    return config.getStringValue(HERON_EXECUTOR_DOCKER_IMAGE, "apache/heron:latest");
  }

  public static boolean getHeronNomadMetricsServiceRegister(Config config) {
    return config.getBooleanValue(HERON_NOMAD_METRICS_SERVICE_REGISTER, false);
  }

  public static int getHeronNomadMetricsServiceCheckIntervalSec(Config config) {
    return config.getIntegerValue(HERON_NOMAD_METRICS_SERVICE_CHECK_INTERVAL_SEC, 10);
  }

  public static int getHeronNomadMetricsServiceCheckTimeoutSec(Config config) {
    return config.getIntegerValue(HERON_NOMAD_METRICS_SERVICE_CHECK_TIMEOUT_SEC, 2);
  }

  public static String[] getHeronNomadMetricsServiceAdditionalTags(Config config) {
    return config.getStringValue(HERON_NOMAD_METRICS_SERVICE_ADDITIONAL_TAGS, "").split(",");
  }

  public static String getHeronNomadNetworkMode(Config config) {
    return config.getStringValue(HERON_NOMAD_NETWORK_MODE, "default");
  }
}
