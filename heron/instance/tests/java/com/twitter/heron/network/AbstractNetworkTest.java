//  Copyright 2017 Twitter. All rights reserved.
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
//  limitations under the License.
package com.twitter.heron.network;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.Before;

import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.instance.CommunicatorTester;
import com.twitter.heron.instance.InstanceControlMsg;
import com.twitter.heron.metrics.GatewayMetrics;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.resource.UnitTestHelper;

/**
 * Common superclass for share setup required for network tests.
 */
public abstract class AbstractNetworkTest {
  static final String HOST = "127.0.0.1";
  protected int serverPort;

  private StreamManagerClient streamManagerClient;
  private CommunicatorTester communicatorTester;
  private GatewayMetrics gatewayMetrics;
  private ExecutorService threadPool;

  static void close(Closeable sc2) {
    if (sc2 != null) {
      try {
        sc2.close();
      } catch (IOException ignored) {
      }
    }
  }

  static void configure(SocketChannel sc) throws SocketException {
    sc.socket().setTcpNoDelay(true);
  }

  @Before
  public void before() throws Exception {
    communicatorTester = new CommunicatorTester();
    gatewayMetrics = new GatewayMetrics();
    threadPool = Executors.newSingleThreadExecutor();

    // Get an available port
    serverPort = SysUtils.getFreePort();
  }

  @After
  public void after() throws Exception {
    communicatorTester.stop();

    if (streamManagerClient != null) {
      streamManagerClient.stop();
    }

    if (threadPool != null) {
      threadPool.shutdownNow();
    }
  }

  protected ExecutorService getThreadPool() {
    return threadPool;
  }

  protected NIOLooper getNIOLooper() {
    return (NIOLooper) communicatorTester.getTestLooper();
  }

  protected Communicator<InstanceControlMsg> getInControlQueue() {
    return communicatorTester.getInControlQueue();
  }

  protected Communicator<HeronTuples.HeronTupleSet> getInStreamQueue() {
    return communicatorTester.getInStreamQueue();
  }

  StreamManagerClient runStreamManagerClient() {
    SystemConfig systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(
            SystemConfig.HERON_SYSTEM_CONFIG);

    HeronSocketOptions socketOptions = new HeronSocketOptions(
         systemConfig.getInstanceNetworkWriteBatchSize(),
         systemConfig.getInstanceNetworkWriteBatchTime(),
         systemConfig.getInstanceNetworkReadBatchSize(),
         systemConfig.getInstanceNetworkReadBatchTime(),
         systemConfig.getInstanceNetworkOptionsSocketSendBufferSize(),
         systemConfig.getInstanceNetworkOptionsSocketReceivedBufferSize());

    final NIOLooper nioLooper = (NIOLooper) communicatorTester.getTestLooper();
    streamManagerClient = new StreamManagerClient(nioLooper, HOST, serverPort,
        "topology-name", "topologyId", UnitTestHelper.getInstance("bolt-id"),
        communicatorTester.getInStreamQueue(), communicatorTester.getOutStreamQueue(),
        communicatorTester.getInControlQueue(), socketOptions, gatewayMetrics);

    Runnable r = new Runnable() {
      @Override
      public void run() {
        try {
          streamManagerClient.start();
          nioLooper.loop();
        } finally {
          streamManagerClient.stop();
          nioLooper.exitLoop();
        }
      }
    };
    threadPool.execute(r);
    return streamManagerClient;
  }
}
