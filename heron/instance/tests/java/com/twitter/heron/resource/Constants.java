package com.twitter.heron.resource;

import org.junit.Ignore;

/**
 * Some casual pre-defined constants used for testing
 */
@Ignore
public class Constants {
  public static final int RETRY_TIMES = 10;
  public static final int RETRY_INTERVAL_MS = 1000;
  public static final int TEST_WAIT_TIME_MS = 2000;

  public static final int QUEUE_BUFFER_SIZE = 128;

  public static final String FAIL_COUNT = "fail-count";
  public static final String ACK_COUNT = "ack-count";
  public static final String ACTIVATE_COUNT = "activate-count";
  public static final String DEACTIVATE_COUNT = "deactivate-count";

  public static final String GATEWAY_METRICS = "com.twitter.heron.metrics.GatewayMetrics";

  public static final String PHYSICAL_PLAN_HELPER = "com.twitter.heron.utility.PhysicalPlanHelper";

  public static final String HERON_SYSTEM_CONFIG = "com.twitter.heron.common.config.SystemConfig";

  public static final String DEFAULT_CONFIG_RELATIVE_PATH = "../heron/config/src/yaml/heron_internals.yaml";

  // For bazel, we use the env var to get the path of heron internals config file
  public static final String BAZEL_TEST_SRCDIR = "TEST_SRCDIR";
  public static final String BAZEL_CONFIG_PATH = "/heron/config/src/yaml/heron_internals.yaml";
}
