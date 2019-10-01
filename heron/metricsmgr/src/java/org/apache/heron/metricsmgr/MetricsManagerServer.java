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

package org.apache.heron.metricsmgr;

import java.net.SocketAddress;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.apache.heron.api.metric.MultiCountMetric;
import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.network.HeronServer;
import org.apache.heron.common.network.HeronSocketOptions;
import org.apache.heron.common.network.REQID;
import org.apache.heron.proto.system.Common;
import org.apache.heron.proto.system.Metrics;
import org.apache.heron.proto.tmaster.TopologyMaster;
import org.apache.heron.spi.metricsmgr.metrics.ExceptionInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsInfo;
import org.apache.heron.spi.metricsmgr.metrics.MetricsRecord;

public class MetricsManagerServer extends HeronServer {
  private static final Logger LOG = Logger.getLogger(MetricsManagerServer.class.getName());

  // Bean name to register the TMasterLocation object into SingletonRegistry
  private static final String TMASTER_LOCATION_BEAN_NAME =
      TopologyMaster.TMasterLocation.newBuilder().getDescriptorForType().getFullName();
  public static final String METRICSCACHE_LOCATION_BEAN_NAME =
      TopologyMaster.MetricsCacheLocation.newBuilder().getDescriptorForType().getFullName();

  // Metrics Counter Name
  private static final String SERVER_CLOSE_PUBLISHER = "close-publisher";
  private static final String SERVER_NEW_REGISTER = "new-register-request";
  private static final String SERVER_METRICS_RECEIVED = "metrics-received";
  private static final String SERVER_EXCEPTIONS_RECEIVED = "exceptions-received";
  private static final String SERVER_NEW_TMASTER_LOCATION = "new-tmaster-location";
  private static final String SERVER_TMASTER_LOCATION_RECEIVED = "tmaster-location-received";
  private static final String SERVER_COMMUNICATOR_OFFER = "communicator-offer";
  private static final String SERVER_COMMUNICATOR_SIZE = "communicator-size";

  private final Map<String, Communicator<MetricsRecord>> metricsSinkCommunicators;

  // A map from MetricPublisher's immutable SocketAddress to the MetricPublisher
  // We would fetch SocketAddress by using SocketChannel.socket().getRemoteSocketAddress,
  // which will continue to return the connected address after the socket is closed.
  // So given that the SocketChannel is connected at first, it would not be null in future.
  private final Map<SocketAddress, Metrics.MetricPublisher> publisherMap;

  // Internal MultiCountMetric Counters
  private final MultiCountMetric serverMetricsCounters;

  /**
   * Constructor
   *
   * @param s the NIOLooper bind with this socket server
   * @param host the host of endpoint to bind with
   * @param port the port of endpoint to bind with
   * @param options the HeronSocketOption for HeronServer
   * @param serverMetricsCounters The MultiCountMetric to update Metircs for MetricsManagerServer
   */
  public MetricsManagerServer(NIOLooper s, String host,
                              int port, HeronSocketOptions options,
                              MultiCountMetric serverMetricsCounters) {
    super(s, host, port, options);

    if (serverMetricsCounters == null) {
      throw new IllegalArgumentException("Server Metrics Counters is needed.");
    }
    this.serverMetricsCounters = serverMetricsCounters;

    // We use CopyOnWriteArrayList to avoid throwing ConcurrentModifiedException
    // Since we might mutate the list while iterating it
    // Consider that the iteration vastly outnumbers mutation,
    // it would barely hurt any performance
    this.metricsSinkCommunicators = Collections.synchronizedMap(new HashMap<>());

    this.publisherMap = new HashMap<SocketAddress, Metrics.MetricPublisher>();

    // Initialize the register
    registerInitialization();
  }

  private void registerInitialization() {
    // Register the RegisterRequest from other instances
    registerOnRequest(Metrics.MetricPublisherRegisterRequest.newBuilder());

    // Register the Metrics Message
    registerOnMessage(Metrics.MetricPublisherPublishMessage.newBuilder());

    // Register the TMasterLocationRefreshMessage, which is used by TMasterSink
    // We do this to avoid communication between TMasterSink and Zookeeper
    // TODO -- Reading TMasterLocationRefreshMessage from StreamMgr is more a temp solution
    // TODO -- It adds dependencies on internal broadcast service
    registerOnMessage(Metrics.TMasterLocationRefreshMessage.newBuilder());
    registerOnMessage(Metrics.MetricsCacheLocationRefreshMessage.newBuilder());
  }

  public void addSinkCommunicator(String id, Communicator<MetricsRecord> communicator) {
    LOG.info("Communicator " + id + " is added: " + communicator);
    synchronized (metricsSinkCommunicators) {
      this.metricsSinkCommunicators.put(id, communicator);
    }
  }

  public boolean removeSinkCommunicator(String id) {
    LOG.info("Remove communicator: " + id);
    boolean found = false;
    synchronized (metricsSinkCommunicators) {
      if (metricsSinkCommunicators.containsKey(id)) {
        this.metricsSinkCommunicators.remove(id);
        found = true;
        LOG.info("Communicator " + id + " is removed");
      }
    }
    return found;
  }

  @Override
  public void onConnect(SocketChannel channel) {
    LOG.info("Metrics Manager got a new connection from host:port "
        + channel.socket().getRemoteSocketAddress());
    // Nothing here. Everything happens in the register
  }

  @Override
  public void onRequest(REQID rid, SocketChannel channel, Message request) {
    if (request instanceof Metrics.MetricPublisherRegisterRequest) {
      handleRegisterRequest(rid, channel, (Metrics.MetricPublisherRegisterRequest) request);
    } else {
      LOG.severe("Unknown kind of request received from Metrics Manager");
    }
  }

  @Override
  public void onMessage(SocketChannel channel, Message message) {
    // Fetch the request to append necessary info
    Metrics.MetricPublisher request = publisherMap.get(channel.socket().getRemoteSocketAddress());
    if (request == null) {
      LOG.severe("Publish message from an unknown socket: " + channel.toString());
      return;
    }

    if (message instanceof Metrics.MetricPublisherPublishMessage) {
      handlePublisherPublishMessage(request, (Metrics.MetricPublisherPublishMessage) message);
    } else if (message instanceof Metrics.MetricsCacheLocationRefreshMessage) {
      // LOG down where the MetricsCache Location comes from
      LOG.info("MetricsCache Location is refresh from: "
          + channel.socket().getRemoteSocketAddress());
      handleMetricsCacheLocationRefreshMessage(
          request, (Metrics.MetricsCacheLocationRefreshMessage) message);
    } else if (message instanceof Metrics.TMasterLocationRefreshMessage) {
      // LOG down where the TMaster Location comes from
      LOG.info("TMaster Location is refresh from: " + channel.socket().getRemoteSocketAddress());
      handleTMasterLocationRefreshMessage(request, (Metrics.TMasterLocationRefreshMessage) message);
    } else {
      LOG.severe("Unknown kind of message received from Metrics Manager");
    }
  }

  @Override
  public void onClose(SocketChannel channel) {
    LOG.log(Level.SEVERE, "Got a connection close from remote socket address: {0}",
        new Object[] {channel.socket().getRemoteSocketAddress()});

    // Unregister the Publisher
    Metrics.MetricPublisher request =
        publisherMap.remove(channel.socket().getRemoteSocketAddress());
    if (request == null) {
      LOG.severe("Unknown connection closed");
    } else {
      LOG.log(Level.SEVERE, "Un-register publish from hostname: {0},"
          + " component_name: {1}, port: {2}, instance_id: {3}, instance_index: {4}",
          new Object[] {request.getHostname(), request.getComponentName(), request.getPort(),
          request.getInstanceId(), request.getInstanceIndex()});
    }

    // Update Metrics
    serverMetricsCounters.scope(SERVER_CLOSE_PUBLISHER).incr();
  }

  // We also allow directly send Metrics Message internally to invoke IMetricsSink
  // This method is thread-safe, since we would push Messages into a Concurrent Queue.
  public void onInternalMessage(Metrics.MetricPublisher request,
                                Metrics.MetricPublisherPublishMessage message) {
    handlePublisherPublishMessage(request, message);
  }

  private void handleRegisterRequest(
        REQID rid,
        SocketChannel channel,
        Metrics.MetricPublisherRegisterRequest request) {
    Metrics.MetricPublisher publisher = request.getPublisher();
    LOG.log(Level.SEVERE, "Got a new register publisher from hostname: {0},"
        + " component_name: {1}, port: {2}, instance_id: {3}, instance_index: {4} from {5}",
        new Object[] {publisher.getHostname(), publisher.getComponentName(), publisher.getPort(),
            publisher.getInstanceId(), publisher.getInstanceIndex(),
            channel.socket().getRemoteSocketAddress()});

    // Check whether publisher has already been registered
    Common.StatusCode responseStatusCode = Common.StatusCode.NOTOK;

    if (publisherMap.containsKey(channel.socket().getRemoteSocketAddress())) {
      LOG.log(Level.SEVERE, "Metrics publisher already exists for hostname: {0},"
          + " component_name: {1}, port: {2}, instance_id: {3}, instance_index: {4}",
          new Object[] {publisher.getHostname(), publisher.getComponentName(), publisher.getPort(),
              publisher.getInstanceId(), publisher.getInstanceIndex()});
    } else {
      publisherMap.put(channel.socket().getRemoteSocketAddress(), publisher);
      // Add it to the map
      responseStatusCode = Common.StatusCode.OK;
    }

    Common.Status responseStatus = Common.Status.newBuilder().setStatus(responseStatusCode).build();
    Metrics.MetricPublisherRegisterResponse response =
        Metrics.MetricPublisherRegisterResponse.newBuilder().setStatus(responseStatus).build();

    // Send the response
    sendResponse(rid, channel, response);

    // Update the Metrics
    serverMetricsCounters.scope(SERVER_NEW_REGISTER).incr();
  }

  private void handlePublisherPublishMessage(Metrics.MetricPublisher request,
                                             Metrics.MetricPublisherPublishMessage message) {
    if (message.getMetricsCount() <= 0 && message.getExceptionsCount() <= 0) {
      LOG.log(Level.SEVERE,
          "Publish message has no metrics nor exceptions for message from hostname: {0},"
          + " component_name: {1}, port: {2}, instance_id: {3}, instance_index: {4}",
          new Object[] {request.getHostname(), request.getComponentName(), request.getPort(),
              request.getInstanceId(), request.getInstanceIndex()});
      return;
    }

    // Convert the message to MetricsRecord
    String source = MetricsUtil.createSource(
        request.getHostname(), request.getPort(),
        request.getComponentName(), request.getInstanceId());

    List<MetricsInfo> metricsInfos = new ArrayList<MetricsInfo>(message.getMetricsCount());
    for (Metrics.MetricDatum metricDatum : message.getMetricsList()) {
      MetricsInfo info = new MetricsInfo(metricDatum.getName(), metricDatum.getValue());
      metricsInfos.add(info);
    }

    List<ExceptionInfo> exceptionInfos = new ArrayList<ExceptionInfo>(message.getExceptionsCount());
    for (Metrics.ExceptionData exceptionData : message.getExceptionsList()) {
      ExceptionInfo exceptionInfo =
          new ExceptionInfo(exceptionData.getStacktrace(),
              exceptionData.getLasttime(),
              exceptionData.getFirsttime(),
              exceptionData.getCount(),
              exceptionData.getLogging());
      exceptionInfos.add(exceptionInfo);
    }

    LOG.info(String.format("%d MetricsInfo and %d ExceptionInfo to push",
        metricsInfos.size(), exceptionInfos.size()));

    // Update the metrics
    serverMetricsCounters.scope(SERVER_METRICS_RECEIVED).incrBy(metricsInfos.size());
    serverMetricsCounters.scope(SERVER_EXCEPTIONS_RECEIVED).incrBy(exceptionInfos.size());


    MetricsRecord record = new MetricsRecord(source, metricsInfos, exceptionInfos);

    // Push MetricsRecord to Communicator, which would wake up SlaveLooper bind with IMetricsSink
    synchronized (metricsSinkCommunicators) {
      Iterator<String> itr = metricsSinkCommunicators.keySet().iterator();
      while (itr.hasNext()) {
        String key = itr.next();
        Communicator<MetricsRecord> c = metricsSinkCommunicators.get(key);
        c.offer(record);
        serverMetricsCounters.scope(SERVER_COMMUNICATOR_OFFER).incr();
        serverMetricsCounters.scope(SERVER_COMMUNICATOR_SIZE + "-" + key).incrBy(c.size());
      }
    }
  }

  // TMasterLocationRefreshMessage handler
  private void handleTMasterLocationRefreshMessage(
      Metrics.MetricPublisher request,
      Metrics.TMasterLocationRefreshMessage tMasterLocationRefreshMessage) {
    TopologyMaster.TMasterLocation oldLocation =
        (TopologyMaster.TMasterLocation)
            SingletonRegistry.INSTANCE.getSingleton(TMASTER_LOCATION_BEAN_NAME);

    TopologyMaster.TMasterLocation newLocation = tMasterLocationRefreshMessage.getTmaster();

    if (oldLocation == null) {
      // The first time to get TMasterLocation

      // Register to the SingletonRegistry
      LOG.info("We received a new TMasterLocation. Register it into SingletonRegistry");
      SingletonRegistry.INSTANCE.registerSingleton(TMASTER_LOCATION_BEAN_NAME, newLocation);

      // Update Metrics
      serverMetricsCounters.scope(SERVER_NEW_TMASTER_LOCATION).incr();

    } else if (oldLocation.equals(newLocation)) {
      // The new one is the same as old one.

      // Just Log. Do nothing
      LOG.info("We received a new TMasterLocation the same as the old one. Do nothing.");
    } else {
      // Have received TMasterLocation earlier, but it changed.

      // We need update the SingletonRegistry
      LOG.info("We received a new TMasterLocation. Replace the old one.");
      LOG.info("Old TMasterLocation: " + oldLocation);
      SingletonRegistry.INSTANCE.updateSingleton(TMASTER_LOCATION_BEAN_NAME, newLocation);

      // Update Metrics
      serverMetricsCounters.scope(SERVER_NEW_TMASTER_LOCATION).incr();
    }

    LOG.info("Current TMaster location: " + newLocation);

    // Update Metrics
    serverMetricsCounters.scope(SERVER_TMASTER_LOCATION_RECEIVED).incr();
  }

  private void handleMetricsCacheLocationRefreshMessage(
      Metrics.MetricPublisher request,
      Metrics.MetricsCacheLocationRefreshMessage tMasterLocationRefreshMessage) {
    TopologyMaster.MetricsCacheLocation oldLocation =
        (TopologyMaster.MetricsCacheLocation)
            SingletonRegistry.INSTANCE.getSingleton(METRICSCACHE_LOCATION_BEAN_NAME);

    TopologyMaster.MetricsCacheLocation newLocation =
        tMasterLocationRefreshMessage.getMetricscache();

    if (oldLocation == null) {
      // The first time to get TMasterLocation

      // Register to the SingletonRegistry
      LOG.info("We received a new MetricsCacheLocation. Register it into SingletonRegistry");
      SingletonRegistry.INSTANCE.registerSingleton(METRICSCACHE_LOCATION_BEAN_NAME, newLocation);

      // Update Metrics
      serverMetricsCounters.scope(SERVER_NEW_TMASTER_LOCATION).incr();

    } else if (oldLocation.equals(newLocation)) {
      // The new one is the same as old one.

      // Just Log. Do nothing
      LOG.info("We received a new MetricsCacheLocation the same as the old one "
          + newLocation + " . Do nothing.");
    } else {
      // Have received TMasterLocation earlier, but it changed.

      // We need update the SingletonRegistry
      LOG.info("We received a new MetricsCacheLocation " + newLocation
          + ". Replace the old one" + oldLocation + ".");
      SingletonRegistry.INSTANCE.updateSingleton(METRICSCACHE_LOCATION_BEAN_NAME, newLocation);

      // Update Metrics
      serverMetricsCounters.scope(SERVER_NEW_TMASTER_LOCATION).incr();
    }

    // Update Metrics
    serverMetricsCounters.scope(SERVER_TMASTER_LOCATION_RECEIVED).incr();
  }
}
