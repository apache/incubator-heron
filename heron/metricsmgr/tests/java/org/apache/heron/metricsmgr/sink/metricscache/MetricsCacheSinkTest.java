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

package org.apache.heron.metricsmgr.sink.metricscache;

import java.lang.reflect.Field;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.heron.api.metric.MultiCountMetric;
import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.config.SystemConfigKey;
import org.apache.heron.metricsmgr.sink.SinkContextImpl;
import org.apache.heron.proto.tmaster.TopologyMaster;
import org.apache.heron.spi.metricsmgr.sink.SinkContext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * MetricsCacheSink Tester.
 */
public class MetricsCacheSinkTest {

  // Bean name to register the MetricsCacheLocation object into SingletonRegistry
  private static final String METRICSCACHE_LOCATION_BEAN_NAME =
      TopologyMaster.MetricsCacheLocation.newBuilder().getDescriptorForType().getFullName();

  private static final Duration RECONNECT_INTERVAL = Duration.ofSeconds(1);
  // Restart wait time is set at 2 times of reconnect time plus another second. The 2 times factor
  // is because of location checking event interval and the sleep of reconnect interval in
  // exception handling.
  private static final Duration RESTART_WAIT_INTERVAL = Duration.ofSeconds(3);
  private static final Duration METRICSCACHE_LOCATION_CHECK_INTERVAL = Duration.ofSeconds(1);

  private static Map<String, Object> buildServiceConfig() {
    Map<String, Object> serviceConfig = new HashMap<>();
    // Fill with necessary config
    serviceConfig.put("reconnect-interval-second", RECONNECT_INTERVAL.getSeconds());
    serviceConfig.put("network-write-batch-size-bytes", 1);
    serviceConfig.put("network-write-batch-time-ms", 1);
    serviceConfig.put("network-read-batch-size-bytes", 1);
    serviceConfig.put("network-read-batch-time-ms", 1);
    serviceConfig.put("socket-send-buffer-size-bytes", 1);
    serviceConfig.put("socket-received-buffer-size-bytes", 1);
    return serviceConfig;
  }

  private static TopologyMaster.MetricsCacheLocation getMetricsCacheLocation(int masterPort) {
    // Notice here we set host and port as invalid values
    // So MetricsCache would throw "java.nio.channels.UnresolvedAddressException" once it starts,
    // and then dies
    return TopologyMaster.MetricsCacheLocation.newBuilder().
        setTopologyName("topology-name").setTopologyId("topology-id").setHost("host").
        setControllerPort(0).setMasterPort(masterPort).setStatsPort(0).build();
  }

  @Before
  public void before() {
    String runFiles = System.getenv("TEST_SRCDIR");
    if (runFiles == null) {
      throw new RuntimeException("Failed to fetch run files resources from built jar");
    }

    String filePath =
        Paths.get(runFiles,
                  "/org_apache_heron/heron/config/src/yaml/conf/test/test_heron_internals.yaml")
             .toString();
    SystemConfig.Builder sb = SystemConfig.newBuilder(true)
        .putAll(filePath, true)
        .put(SystemConfigKey.HERON_METRICS_EXPORT_INTERVAL, 1);
    SingletonRegistry.INSTANCE.registerSingleton("org.apache.heron.common.config.SystemConfig",
                                                 sb.build());
  }

  @After
  @SuppressWarnings("unchecked")
  public void after() throws Exception {
    // Remove the Singleton by Reflection
    Field field = SingletonRegistry.INSTANCE.getClass().getDeclaredField("singletonObjects");
    field.setAccessible(true);
    Map<String, Object> singletonObjects =
        (Map<String, Object>) field.get(SingletonRegistry.INSTANCE);
    singletonObjects.clear();
  }

  /**
   * Test automatic recover from uncaught exceptions in MetricsCacheClient
   */
  @Test
  public void testMetricsCacheClientService() throws Exception {
    // create a new MetricsCacheClientService
    MetricsCacheSink metricsCacheSink = new MetricsCacheSink();
    Map<String, Object> serviceConfig = buildServiceConfig();

    metricsCacheSink.createSimpleMetricsCacheClientService(serviceConfig);
    metricsCacheSink.startNewMetricsCacheClient(getMetricsCacheLocation(0));

    // We wait for a while to let auto recover fully finish.
    SysUtils.sleep(RESTART_WAIT_INTERVAL);

    // Then we check whether the MetricsCacheService has restarted the MetricsCacheClient for
    // several times Take other factors into account, we would check whether the MetricsCacheClient
    // has restarted at least half the RESTART_WAIT_INTERVAL_SECONDS/RECONNECT_INTERVAL
    assertTrue(metricsCacheSink.getMetricsCacheStartedAttempts()
        >= (RESTART_WAIT_INTERVAL.getSeconds() / RECONNECT_INTERVAL.getSeconds() / 2));
    metricsCacheSink.close();
  }

  /**
   * Test whether MetricsCacheSink would handle MetricsCacheLocation in SingletonRegistry automatically
   */
  @Test
  public void testHandleMetricsCacheLocation() throws Exception {
    // create a new MetricsCacheClientService
    MetricsCacheSink metricsCacheSink = new MetricsCacheSink();
    Map<String, Object> sinkConfig = new HashMap<String, Object>();

    // Fill with necessary config
    sinkConfig.put("metricscache-location-check-interval-sec",
        METRICSCACHE_LOCATION_CHECK_INTERVAL.getSeconds());

    sinkConfig.put("metricscache-client",  buildServiceConfig());

    // It is null since we have not set it
    Assert.assertNull(metricsCacheSink.getCurrentMetricsCacheLocation());

    MultiCountMetric multiCountMetric = new MultiCountMetric();
    SinkContext sinkContext =
        new SinkContextImpl("topology-name", "cluster", "role", "environment",
            "metricsmgr-id", "sink-id", multiCountMetric);

    // Start the MetricsCacheSink
    metricsCacheSink.init(sinkConfig, sinkContext);

    // Put the MetricsCacheLocation into SingletonRegistry
    TopologyMaster.MetricsCacheLocation oldLoc = getMetricsCacheLocation(0);
    SingletonRegistry.INSTANCE.registerSingleton(METRICSCACHE_LOCATION_BEAN_NAME, oldLoc);

    SysUtils.sleep(RESTART_WAIT_INTERVAL);

    // The MetricsCacheService should start
    assertTrue(metricsCacheSink.getMetricsCacheStartedAttempts() > 0);
    assertEquals(oldLoc, metricsCacheSink.getCurrentMetricsCacheLocation());
    assertEquals(oldLoc, metricsCacheSink.getCurrentMetricsCacheLocationInService());

    // Update it, the MetricsCacheSink should pick up the new one.
    TopologyMaster.MetricsCacheLocation newLoc = getMetricsCacheLocation(1);
    SingletonRegistry.INSTANCE.updateSingleton(METRICSCACHE_LOCATION_BEAN_NAME, newLoc);

    int lastMetricsCacheStartedAttempts = metricsCacheSink.getMetricsCacheStartedAttempts();

    SysUtils.sleep(RESTART_WAIT_INTERVAL);

    // The MetricsCacheService should use the new MetricsCacheLocation
    assertTrue(
        metricsCacheSink.getMetricsCacheStartedAttempts() > lastMetricsCacheStartedAttempts);
    assertNotSame(oldLoc, metricsCacheSink.getCurrentMetricsCacheLocation());
    assertNotSame(oldLoc, metricsCacheSink.getCurrentMetricsCacheLocationInService());
    assertEquals(newLoc, metricsCacheSink.getCurrentMetricsCacheLocation());
    assertEquals(newLoc, metricsCacheSink.getCurrentMetricsCacheLocationInService());

    metricsCacheSink.close();
  }

  @Test
  public void testCheckCommunicator() {
    Communicator<TopologyMaster.PublishMetrics> communicator = new Communicator<>();
    int initSize = 16;
    int capSize = 10;

    TopologyMaster.PublishMetrics.Builder publishMetrics =
        TopologyMaster.PublishMetrics.newBuilder();
    for (int i = 0; i < initSize; ++i) {
      communicator.offer(publishMetrics.build());
    }
    assertEquals(communicator.size(), initSize);

    MetricsCacheSink.checkCommunicator(communicator, initSize + 1);
    assertEquals(communicator.size(), initSize);

    MetricsCacheSink.checkCommunicator(communicator, initSize);
    assertEquals(communicator.size(), initSize);

    MetricsCacheSink.checkCommunicator(communicator, initSize - 1);
    assertEquals(communicator.size(), initSize - 1);

    MetricsCacheSink.checkCommunicator(communicator, capSize);
    assertEquals(communicator.size(), capSize);
  }
}
