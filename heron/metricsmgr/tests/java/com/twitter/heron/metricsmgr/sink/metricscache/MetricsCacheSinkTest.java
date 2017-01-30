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

package com.twitter.heron.metricsmgr.sink.metricscache;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.twitter.heron.api.metric.MultiCountMetric;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.metricsmgr.sink.SinkContextImpl;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.metricsmgr.sink.SinkContext;

/**
 * MetricsCacheSink Tester.
 */
public class MetricsCacheSinkTest {

  // Bean name to register the MetricsCacheLocation object into SingletonRegistry
  private static final String METRICSCACHE_LOCATION_BEAN_NAME =
      TopologyMaster.MetricsCacheLocation.newBuilder().getDescriptorForType().getFullName();

  private static final int RECONNECT_INTERVAL_SECONDS = 1;
  private static final int RESTART_WAIT_INTERVAL_SECONDS = 5;
  private static final int METRICSCACHE_LOCATION_CHECK_INTERVAL_SECONDS = 1;
  private static final int WAIT_SECONDS = 10;

  @Before
  public void before() throws Exception {
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
    Map<String, Object> serviceConfig = new HashMap<String, Object>();
    // Fill with necessary config
    serviceConfig.put("reconnect-interval-second", RECONNECT_INTERVAL_SECONDS);
    serviceConfig.put("network-write-batch-size-bytes", 1);
    serviceConfig.put("network-write-batch-time-ms", 1);
    serviceConfig.put("network-read-batch-size-bytes", 1);
    serviceConfig.put("network-read-batch-time-ms", 1);
    serviceConfig.put("socket-send-buffer-size-bytes", 1);
    serviceConfig.put("socket-received-buffer-size-bytes", 1);

    metricsCacheSink.createSimpleMetricsCacheClientService(serviceConfig);

    // Notice here we set host and port as invalid values
    // So MetricsCache would throw "java.nio.channels.UnresolvedAddressException" once it starts,
    // and then dies
    TopologyMaster.MetricsCacheLocation location = TopologyMaster.MetricsCacheLocation.newBuilder().
        setTopologyName("topology-name").setTopologyId("topology-id").setHost("host").
        setControllerPort(0).setMasterPort(0).setStatsPort(0).build();
    metricsCacheSink.startNewMetricsCacheClient(location);

    // We wait for a while to let auto recover fully finish.
    Thread.sleep(RESTART_WAIT_INTERVAL_SECONDS * 1000);

    // Then we check whether the MetricsCacheService has restarted the MetricsCacheClient for
    // several times Take other factors into account, we would check whether the MetricsCacheClient
    // has restarted at least half the RESTART_WAIT_INTERVAL_SECONDS/RECONNECT_INTERVAL_SECONDS
    Assert.assertTrue(metricsCacheSink.getMetricsCacheStartedAttempts()
        > (RESTART_WAIT_INTERVAL_SECONDS / RECONNECT_INTERVAL_SECONDS / 2));
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
    sinkConfig.put(
        "metricscache-location-check-interval-sec", METRICSCACHE_LOCATION_CHECK_INTERVAL_SECONDS);

    // These are config for MetricsCacheClient
    Map<String, Object> serviceConfig = new HashMap<String, Object>();
    serviceConfig.put("reconnect-interval-second", RECONNECT_INTERVAL_SECONDS);
    serviceConfig.put("network-write-batch-size-bytes", 1);
    serviceConfig.put("network-write-batch-time-ms", 1);
    serviceConfig.put("network-read-batch-size-bytes", 1);
    serviceConfig.put("network-read-batch-time-ms", 1);
    serviceConfig.put("socket-send-buffer-size-bytes", 1);
    serviceConfig.put("socket-received-buffer-size-bytes", 1);

    sinkConfig.put("metricscache-client", serviceConfig);

    // It is null since we have not set it
    Assert.assertNull(metricsCacheSink.getCurrentMetricsCacheLocation());

    SinkContext sinkContext =
        new SinkContextImpl("topology-name", "metricsmgr-id", "sink-id", new MultiCountMetric());

    // Start the MetricsCacheSink
    metricsCacheSink.init(sinkConfig, sinkContext);

    // Put the MetricsCacheLocation into SingletonRegistry
    TopologyMaster.MetricsCacheLocation oldLoc = TopologyMaster.MetricsCacheLocation.newBuilder().
        setTopologyName("topology-name").setTopologyId("topology-id").
        setHost("host").setControllerPort(0).setMasterPort(0).build();
    SingletonRegistry.INSTANCE.registerSingleton(METRICSCACHE_LOCATION_BEAN_NAME, oldLoc);

    Thread.sleep(WAIT_SECONDS * 1000);

    // The MetricsCacheService should start
    Assert.assertTrue(metricsCacheSink.getMetricsCacheStartedAttempts() > 0);
    Assert.assertEquals(oldLoc, metricsCacheSink.getCurrentMetricsCacheLocation());
    Assert.assertEquals(oldLoc, metricsCacheSink.getCurrentMetricsCacheLocationInService());

    // Update it, the MetricsCacheSink should pick up the new one.
    TopologyMaster.MetricsCacheLocation newLoc = TopologyMaster.MetricsCacheLocation.newBuilder().
        setTopologyName("topology-name").setTopologyId("topology-id").
        setHost("host").setControllerPort(0).setMasterPort(1).build();
    SingletonRegistry.INSTANCE.updateSingleton(METRICSCACHE_LOCATION_BEAN_NAME, newLoc);

    int lastMetricsCacheStartedAttempts = metricsCacheSink.getMetricsCacheStartedAttempts();

    Thread.sleep(WAIT_SECONDS * 1000);

    // The MetricsCacheService should use the new MetricsCacheLocation
    Assert.assertTrue(
        metricsCacheSink.getMetricsCacheStartedAttempts() > lastMetricsCacheStartedAttempts);
    Assert.assertNotSame(oldLoc, metricsCacheSink.getCurrentMetricsCacheLocation());
    Assert.assertNotSame(oldLoc, metricsCacheSink.getCurrentMetricsCacheLocationInService());
    Assert.assertEquals(newLoc, metricsCacheSink.getCurrentMetricsCacheLocation());
    Assert.assertEquals(newLoc, metricsCacheSink.getCurrentMetricsCacheLocationInService());

    metricsCacheSink.close();
  }
}
