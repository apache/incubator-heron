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
 */
public class MetricsCacheManagerServer extends HeronServer {
  private static final Logger LOG = Logger.getLogger(MetricsManagerServer.class.getName());

  private MetricsCache metricsCache = null;

  /**
   * Constructor
   *
   * @param s the NIOLooper bind with this socket server
   * @param host the host of remote endpoint to communicate with
   * @param port the port of remote endpoint to communicate with
   */
  public MetricsCacheManagerServer(NIOLooper s, String host, int port, HeronSocketOptions options,
                                   MetricsCache cache) {
    super(s, host, port, options);

    metricsCache = cache;
  }

  @Override
  public void onConnect(SocketChannel channel) {
    LOG.info("MetricsCacheManagerServer onConnect from host:port "
        + channel.socket().getRemoteSocketAddress());
  }

  @Override
  public void onRequest(REQID rid, SocketChannel channel, Message request) {
    LOG.info("MetricsCacheManagerServer onRequest from host:port "
        + channel.socket().getRemoteSocketAddress());

    if (request instanceof TopologyMaster.MetricRequest) {
      TopologyMaster.MetricResponse resp =
          metricsCache.GetMetrics((TopologyMaster.MetricRequest) request);
      LOG.info("query finished, to send response");
      sendResponse(rid, channel, resp);
      LOG.info("queued response size " + resp.getSerializedSize());
    } else {
      LOG.severe("Unknown kind of request received");
    }
  }

  @Override
  public void onMessage(SocketChannel channel, Message message) {
    LOG.info("MetricsCacheManagerServer onMessage from host:port "
        + channel.socket().getRemoteSocketAddress());

    if (message instanceof TopologyMaster.PublishMetrics) {
      metricsCache.AddMetric((TopologyMaster.PublishMetrics) message);
    } else {
      LOG.severe("Unknown kind of message received");
    }
  }

  @Override
  public void onClose(SocketChannel channel) {
    LOG.info("MetricsCacheManagerServer onClose from host:port "
        + channel.socket().getRemoteSocketAddress());
  }
}
