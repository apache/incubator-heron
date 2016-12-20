//  Copyright 2016 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License
package com.twitter.heron.metricscachemgr;

import java.nio.channels.SocketChannel;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.network.HeronServer;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.network.REQID;
import com.twitter.heron.metricsmgr.MetricsManagerServer;

/**
 * server to accept metrics from a particular sink in metrics manager
 */
public class MetricsCacheManagerServer extends HeronServer {
  private static final Logger LOG = Logger.getLogger(MetricsManagerServer.class.getName());

  /**
   * Constructor
   *
   * @param s the NIOLooper bind with this socket server
   * @param host the host of remote endpoint to communicate with
   * @param port the port of remote endpoint to communicate with
   */
  public MetricsCacheManagerServer(NIOLooper s, String host, int port, HeronSocketOptions options) {
    super(s, host, port, options);
  }

  @Override
  public void onConnect(SocketChannel channel) {

  }

  @Override
  public void onRequest(REQID rid, SocketChannel channel, Message request) {

  }

  @Override
  public void onMessage(SocketChannel channel, Message message) {

  }

  @Override
  public void onClose(SocketChannel channel) {

  }
}
