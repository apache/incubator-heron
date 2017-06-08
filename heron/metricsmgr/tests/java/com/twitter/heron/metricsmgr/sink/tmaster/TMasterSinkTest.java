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

package com.twitter.heron.metricsmgr.sink.tmaster;

import java.lang.reflect.Field;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.metricsmgr.LatchedMultiCountMetric;
import com.twitter.heron.metricsmgr.sink.SinkContextImpl;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.metricsmgr.sink.SinkContext;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

/**
 * TMasterSink Tester.
 */
public class TMasterSinkTest {

  // Bean name to register the TMasterLocation object into SingletonRegistry
  private static final String TMASTER_LOCATION_BEAN_NAME =
      TopologyMaster.TMasterLocation.newBuilder().getDescriptorForType().getFullName();

  private static final Duration RECONNECT_INTERVAL = Duration.ofSeconds(1);
  private static final Duration RESTART_WAIT_INTERVAL = Duration.ofSeconds(2);
  private static final Duration TMASTER_LOCATION_CHECK_INTERVAL = Duration.ofSeconds(1);

  // These are config for TMasterClient
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

  private static TopologyMaster.TMasterLocation getTMasterLocation(int masterPort) {
    // Notice here we set host and port as invalid values
    // So TMaster would throw "java.nio.channels.UnresolvedAddressException" once it starts,
    // and then dies
    return TopologyMaster.TMasterLocation.newBuilder().
        setTopologyName("topology-name").setTopologyId("topology-id").setHost("host").
        setControllerPort(0).setMasterPort(masterPort).setStatsPort(0).build();
  }

  @Before
  public void before() {
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
   * Test automatic recover from uncaught exceptions in TMasterClient
   */
  @Test
  public void testTMasterClientService() throws InterruptedException {
    // create a new TMasterClientService
    TMasterSink tMasterSink = new TMasterSink();
    tMasterSink.createSimpleTMasterClientService(buildServiceConfig());
    tMasterSink.startNewTMasterClient(getTMasterLocation(0));

    // We wait for a while to let auto recover fully finish.
    SysUtils.sleep(RESTART_WAIT_INTERVAL);

    // Then we check whether the TMasterService has restarted the TMasterClient for several times
    // Take other factors into account, we would check whether the TMasterClient has restarted
    // at least half the RESTART_WAIT_INTERVAL/RECONNECT_INTERVAL
    assertTrue(tMasterSink.getTMasterStartedAttempts()
        > (RESTART_WAIT_INTERVAL.getSeconds() / RECONNECT_INTERVAL.getSeconds() / 2));
    tMasterSink.close();
  }

  /**
   * Test whether TMasterSink would handle TMasterLocation in SingletonRegistry automatically
   */
  @Test
  public void testHandleTMasterLocation() throws InterruptedException {
    // create a new TMasterClientService
    TMasterSink tMasterSink = new TMasterSink();
    Map<String, Object> sinkConfig = new HashMap<>();

    // Fill with necessary config
    sinkConfig.put(
        "tmaster-location-check-interval-sec", TMASTER_LOCATION_CHECK_INTERVAL.getSeconds());

    sinkConfig.put("tmaster-client", buildServiceConfig());

    // It is null since we have not set it
    Assert.assertNull(tMasterSink.getCurrentTMasterLocation());

    LatchedMultiCountMetric multiCountMetric =
        new LatchedMultiCountMetric("tmaster-location-update-count", 1L, 2L);
    SinkContext sinkContext =
        new SinkContextImpl("topology-name", "metricsmgr-id", "sink-id", multiCountMetric);

    // Start the TMasterSink
    tMasterSink.init(sinkConfig, sinkContext);

    // Put the TMasterLocation into SingletonRegistry
    TopologyMaster.TMasterLocation oldLoc = getTMasterLocation(0);
    SingletonRegistry.INSTANCE.registerSingleton(TMASTER_LOCATION_BEAN_NAME, oldLoc);

    multiCountMetric.await(RESTART_WAIT_INTERVAL);

    // The TMasterService should start
    assertTrue(tMasterSink.getTMasterStartedAttempts() > 0);
    assertEquals(oldLoc, tMasterSink.getCurrentTMasterLocation());
    assertEquals(oldLoc, tMasterSink.getCurrentTMasterLocationInService());

    // Update it, the TMasterSink should pick up the new one.
    TopologyMaster.TMasterLocation newLoc = getTMasterLocation(1);
    SingletonRegistry.INSTANCE.updateSingleton(TMASTER_LOCATION_BEAN_NAME, newLoc);

    int lastTMasterStartedAttempts = tMasterSink.getTMasterStartedAttempts();

    multiCountMetric.await(RESTART_WAIT_INTERVAL);

    // The TMasterService should use the new TMasterLocation
    assertTrue(tMasterSink.getTMasterStartedAttempts() > lastTMasterStartedAttempts);
    assertNotSame(oldLoc, tMasterSink.getCurrentTMasterLocation());
    assertNotSame(oldLoc, tMasterSink.getCurrentTMasterLocationInService());
    assertEquals(newLoc, tMasterSink.getCurrentTMasterLocation());
    assertEquals(newLoc, tMasterSink.getCurrentTMasterLocationInService());

    tMasterSink.close();
  }
}

