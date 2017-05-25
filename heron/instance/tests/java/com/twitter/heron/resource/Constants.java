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

package com.twitter.heron.resource;

import java.time.Duration;

import org.junit.Ignore;

/**
 * Some casual pre-defined constants used for testing
 */
@Ignore
public final class Constants {
  public static final int RETRY_TIMES = 10;
  public static final Duration RETRY_INTERVAL = Duration.ofSeconds(1);
  public static final Duration TEST_WAIT_TIME = Duration.ofSeconds(2);

  public static final int QUEUE_BUFFER_SIZE = 128;

  public static final String EXECUTE_COUNT = "execute-count";
  public static final String FAIL_COUNT = "fail-count";
  public static final String ACK_COUNT = "ack-count";

  public static final String EXECUTE_LATCH = "execute-latch";
  public static final String FAIL_LATCH = "fail-latch";
  public static final String ACK_LATCH = "ack-latch";

  public static final String ACTIVATE_COUNT_LATCH = "activate-count-latch";
  public static final String DEACTIVATE_COUNT_LATCH = "deactivate-count-latch";

  public static final String GATEWAY_METRICS = "com.twitter.heron.metrics.GatewayMetrics";

  public static final String PHYSICAL_PLAN_HELPER = "com.twitter.heron.utility.PhysicalPlanHelper";

  public static final String HERON_SYSTEM_CONFIG = "com.twitter.heron.common.config.SystemConfig";

  // For bazel, we use the env var to get the path of heron internals config file
  public static final String BUILD_TEST_SRCDIR = "TEST_SRCDIR";
  public static final String BUILD_TEST_HERON_INTERNALS_CONFIG_PATH =
      "/__main__/heron/config/src/yaml/conf/test/test_heron_internals.yaml";

  private Constants() {
  }
}
