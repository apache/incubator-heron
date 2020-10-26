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

package org.apache.heron.metricscachemgr;

import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.network.HeronServer;
import org.apache.heron.common.network.HeronSocketOptions;
import org.apache.heron.common.network.REQID;
import org.apache.heron.metricscachemgr.metricscache.MetricsCache;
import org.apache.heron.metricsmgr.MetricsManagerServer;
import org.apache.heron.proto.tmanager.TopologyManager;

/**
 * server to accept metrics from a particular sink in metrics manager
 * <p>
 * Differece from MetricsCacheManagerHttpServer:
 * 1. MetricsCacheManagerServer accepts metric publishing message from sinks;
 * MetricsCacheManagerHttpServer responds to queries.
 * 2. MetricsCacheManagerServer is a HeronServer;
 * MetricsCacheManagerHttpServer is a http server
 */
public class MetricsCacheManagerServer extends HeronServer {
  private static final Logger LOG = Logger.getLogger(MetricsManagerServer.class.getName());

  private final MetricsCache metricsCache;

  /**
   * Constructor
   *
   * @param looper the NIOLooper bind with this socket server
   * @param host the host of remote endpoint to communicate with
   * @param port the port of remote endpoint to communicate with
   */
  public MetricsCacheManagerServer(NIOLooper looper, String host, int port,
                                   HeronSocketOptions options, MetricsCache cache) {
    super(looper, host, port, options);

    metricsCache = cache;
  }

  @Override
  public void onConnect(SocketChannel channel) {
    LOG.fine("MetricsCacheManagerServer onConnect from host:port "
        + channel.socket().getRemoteSocketAddress());
  }

  @Override
  public void onRequest(REQID requestId, SocketChannel channel, Message request) {
    LOG.fine("MetricsCacheManagerServer onRequest from host:port "
        + channel.socket().getRemoteSocketAddress());

    if (request instanceof TopologyManager.MetricRequest) {
      LOG.fine("received request " + (TopologyManager.MetricRequest) request);
      TopologyManager.MetricResponse resp =
          metricsCache.getMetrics((TopologyManager.MetricRequest) request);
      LOG.fine("query finished, to send response");
      sendResponse(requestId, channel, resp);
      LOG.fine("queued response size " + resp.getSerializedSize());
    } else {
      LOG.severe("Unknown kind of request received "
          + channel.socket().getRemoteSocketAddress() + "; " + request);
    }
  }

  @Override
  public void onMessage(SocketChannel channel, Message message) {
    LOG.fine("MetricsCacheManagerServer onMessage from host:port "
        + channel.socket().getRemoteSocketAddress());

    if (message instanceof TopologyManager.PublishMetrics) {
      LOG.fine("received message " + (TopologyManager.PublishMetrics) message);
      metricsCache.addMetrics((TopologyManager.PublishMetrics) message);
    } else {
      LOG.severe("Unknown kind of message received "
          + channel.socket().getRemoteSocketAddress() + "; " + message);
    }
  }

  @Override
  public void onClose(SocketChannel channel) {
    LOG.fine("MetricsCacheManagerServer onClose from host:port "
        + channel.socket().getRemoteSocketAddress());
  }
}
