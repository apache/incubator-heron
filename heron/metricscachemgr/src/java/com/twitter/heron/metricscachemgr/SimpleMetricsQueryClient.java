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

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import com.google.protobuf.Message;

import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.network.HeronClient;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.network.StatusCode;
import com.twitter.heron.proto.tmaster.TopologyMaster;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.statemgr.localfs.LocalFileSystemStateManager;

/**
 * example: query metricscache
 */
public class SimpleMetricsQueryClient extends HeronClient {
  /**
   * Constructor
   */
  private String topo;
  private String component;

  public SimpleMetricsQueryClient(NIOLooper s, String host, int port, HeronSocketOptions options,
                                  String topo, String component) {
    super(s, host, port, options);
    this.topo = topo;
    this.component = component;
  }

  public static void main(String[] args)
      throws ExecutionException, InterruptedException, IOException {
    if (args.length < 2) {
      System.out.println(
          "Usage: java MetricsQueryExample <topology_name> <component_name>");
    } else {
      System.out.println("topology: " + args[0] + "; component: " + args[1]);
    }

    Config config = Config.newBuilder()
        .put(Keys.stateManagerRootPath(),
            System.getProperty("user.home") + "/.herondata/repository/state/local")
        .build();
    LocalFileSystemStateManager stateManager = new LocalFileSystemStateManager();
    stateManager.initialize(config);

    TopologyMaster.MetricsCacheLocation location =
        stateManager.getMetricsCacheLocation(null, args[0]).get();

    NIOLooper looper = new NIOLooper();
    HeronClient client = new SimpleMetricsQueryClient(looper,
        location.getHost(), location.getMasterPort(), new HeronSocketOptions(
        32768, 16, 32768,
        16, 6553600, 8738000),
        args[0], args[1]);
    client.start();
    looper.loop();
  }

  @Override
  public void onError() {
    System.out.println("onError");
  }

  @Override
  public void onConnect(StatusCode status) {
    System.out.println("onConnect");
    Message request = TopologyMaster.MetricRequest.newBuilder()
        .setComponentName(component).setMinutely(true).setInterval(-1)
        .build();
    Message.Builder responseBuilder = TopologyMaster.MetricResponse.newBuilder();
    sendRequest(request, responseBuilder);
  }

  @Override
  public void onResponse(StatusCode status, Object ctx, Message response) {
    System.out.println("onResponse");
    if (status.equals(StatusCode.OK)) {
      if (response instanceof TopologyMaster.MetricResponse) {
        System.out.println("response size " + response.getSerializedSize());
        System.out.println("response content " + (TopologyMaster.MetricResponse) response);
      } else {
        System.out.println("unknown response" + response);
      }
    } else {
      System.out.println("response " + status);
    }
    stop();
  }

  @Override
  public void onIncomingMessage(Message message) {
    System.out.println("onIncomingMessage");
  }

  @Override
  public void onClose() {
    System.out.println("onClose");
    getNIOLooper().exitLoop();
  }
}

