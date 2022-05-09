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

package org.apache.heron.metricsmgr.sink.tmanager;

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
import org.apache.heron.proto.tmanager.TopologyManager;
import org.apache.heron.spi.metricsmgr.sink.SinkContext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * TManagerSink Tester.
 */
public class TManagerSinkTest {

  // Bean name to register the TManagerLocation object into SingletonRegistry
  private static final String TMANAGER_LOCATION_BEAN_NAME =
      TopologyManager.TManagerLocation.newBuilder().getDescriptorForType().getFullName();

  private static final Duration RECONNECT_INTERVAL = Duration.ofSeconds(1);
  // Restart wait time is set at 2 times of reconnect time plus another second. The 2 times factor
  // is because of location checking event interval and the sleep of reconnect interval in
  // exception handling.
  private static final Duration RESTART_WAIT_INTERVAL = Duration.ofSeconds(3);
  private static final Duration TMANAGER_LOCATION_CHECK_INTERVAL = Duration.ofSeconds(1);

  // These are config for TManagerClient
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

  private static TopologyManager.TManagerLocation getTManagerLocation(int serverPort) {
    // Notice here we set host and port as invalid values
    // So TManager would throw "java.nio.channels.UnresolvedAddressException" once it starts,
    // and then dies
    return TopologyManager.TManagerLocation.newBuilder().
        setTopologyName("topology-name").setTopologyId("topology-id").setHost("host").
        setControllerPort(0).setServerPort(serverPort).setStatsPort(0).build();
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
  public void after() throws NoSuchFieldException, IllegalAccessException {
    // Remove the Singleton by Reflection
    Field field = SingletonRegistry.INSTANCE.getClass().getDeclaredField("singletonObjects");
    field.setAccessible(true);
    Map<String, Object> singletonObjects =
        (Map<String, Object>) field.get(SingletonRegistry.INSTANCE);
    singletonObjects.clear();
  }

  /**
   * Test automatic recover from uncaught exceptions in TManagerClient
   */
  @Test
  public void testTManagerClientService() throws InterruptedException {
    // create a new TManagerClientService
    TManagerSink tManagerSink = new TManagerSink();
    tManagerSink.createSimpleTManagerClientService(buildServiceConfig());
    tManagerSink.startNewTManagerClient(getTManagerLocation(0));

    // We wait for a while to let auto recover fully finish.
    SysUtils.sleep(RESTART_WAIT_INTERVAL);

    // Then we check whether the TManagerService has restarted the TManagerClient for several times
    // Take other factors into account, we would check whether the TManagerClient has restarted
    // at least half the RESTART_WAIT_INTERVAL/RECONNECT_INTERVAL
    assertTrue(tManagerSink.getTManagerStartedAttempts()
        >= (RESTART_WAIT_INTERVAL.getSeconds() / RECONNECT_INTERVAL.getSeconds() / 2));
    tManagerSink.close();
  }

  /**
   * Test whether TManagerSink would handle TManagerLocation in SingletonRegistry automatically
   */
  @Test
  public void testHandleTManagerLocation() throws InterruptedException {
    // create a new TManagerClientService
    TManagerSink tManagerSink = new TManagerSink();
    Map<String, Object> sinkConfig = new HashMap<>();

    // Fill with necessary config
    sinkConfig.put(
        "tmanager-location-check-interval-sec", TMANAGER_LOCATION_CHECK_INTERVAL.getSeconds());

    sinkConfig.put("tmanager-client", buildServiceConfig());

    // It is null since we have not set it
    Assert.assertNull(tManagerSink.getCurrentTManagerLocation());

    MultiCountMetric multiCountMetric = new MultiCountMetric();
    SinkContext sinkContext =
        new SinkContextImpl("topology-name", "cluster", "role", "environment",
            "metricsmgr-id", "sink-id", multiCountMetric);

    // Start the TManagerSink
    tManagerSink.init(sinkConfig, sinkContext);

    // Put the TManagerLocation into SingletonRegistry
    TopologyManager.TManagerLocation oldLoc = getTManagerLocation(0);
    SingletonRegistry.INSTANCE.registerSingleton(TMANAGER_LOCATION_BEAN_NAME, oldLoc);

    SysUtils.sleep(RESTART_WAIT_INTERVAL);

    // The TManagerService should start
    assertTrue(tManagerSink.getTManagerStartedAttempts() > 0);
    assertEquals(oldLoc, tManagerSink.getCurrentTManagerLocation());
    assertEquals(oldLoc, tManagerSink.getCurrentTManagerLocationInService());

    // Update it, the TManagerSink should pick up the new one.
    TopologyManager.TManagerLocation newLoc = getTManagerLocation(1);
    SingletonRegistry.INSTANCE.updateSingleton(TMANAGER_LOCATION_BEAN_NAME, newLoc);

    int lastTManagerStartedAttempts = tManagerSink.getTManagerStartedAttempts();

    SysUtils.sleep(RESTART_WAIT_INTERVAL);

    // The TManagerService should use the new TManagerLocation
    assertTrue(tManagerSink.getTManagerStartedAttempts() > lastTManagerStartedAttempts);
    assertNotSame(oldLoc, tManagerSink.getCurrentTManagerLocation());
    assertNotSame(oldLoc, tManagerSink.getCurrentTManagerLocationInService());
    assertEquals(newLoc, tManagerSink.getCurrentTManagerLocation());
    assertEquals(newLoc, tManagerSink.getCurrentTManagerLocationInService());

    tManagerSink.close();
  }

  @Test
  public void testCheckCommunicator() {
    Communicator<TopologyManager.PublishMetrics> communicator = new Communicator<>();
    int initSize = 16;
    int capSize = 10;

    TopologyManager.PublishMetrics.Builder publishMetrics =
        TopologyManager.PublishMetrics.newBuilder();
    for (int i = 0; i < initSize; ++i) {
      communicator.offer(publishMetrics.build());
    }
    assertEquals(communicator.size(), initSize);

    TManagerSink.checkCommunicator(communicator, initSize + 1);
    assertEquals(communicator.size(), initSize);

    TManagerSink.checkCommunicator(communicator, initSize);
    assertEquals(communicator.size(), initSize);

    TManagerSink.checkCommunicator(communicator, initSize - 1);
    assertEquals(communicator.size(), initSize - 1);

    TManagerSink.checkCommunicator(communicator, capSize);
    assertEquals(communicator.size(), capSize);
  }
}

