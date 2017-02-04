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

package com.twitter.heron.metricscachemgr;

import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.network.HeronServer;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.network.REQID;
import com.twitter.heron.metricscachemgr.metricscache.MetricsCache;
import com.twitter.heron.metricsmgr.MetricsManagerServer;
import com.twitter.heron.proto.tmaster.TopologyMaster;

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

  private MetricsCache metricsCache = null;

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

    if (request instanceof TopologyMaster.MetricRequest) {
      LOG.fine("received request " + (TopologyMaster.MetricRequest) request);
      TopologyMaster.MetricResponse resp =
          metricsCache.getMetrics((TopologyMaster.MetricRequest) request);
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

    if (message instanceof TopologyMaster.PublishMetrics) {
      LOG.info("received message " + (TopologyMaster.PublishMetrics) message);
      metricsCache.addMetrics((TopologyMaster.PublishMetrics) message);
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
