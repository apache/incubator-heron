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

package org.apache.heron.network;

import java.io.Closeable;
import java.io.IOException;
import java.net.SocketException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.protobuf.Message;

import org.junit.After;
import org.junit.Before;

import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.network.HeronSocketOptions;
import org.apache.heron.common.network.IncomingPacket;
import org.apache.heron.common.testhelpers.HeronServerTester;
import org.apache.heron.instance.CommunicatorTester;
import org.apache.heron.instance.InstanceControlMsg;
import org.apache.heron.metrics.GatewayMetrics;
import org.apache.heron.resource.UnitTestHelper;

import static org.junit.Assert.assertEquals;

/**
 * Common superclass to share setup required for network tests.
 */
public abstract class AbstractNetworkTest {
  static final String HOST = "127.0.0.1";
  private int serverPort;

  private StreamManagerClient streamManagerClient;
  private CommunicatorTester communicatorTester;
  private GatewayMetrics gatewayMetrics;
  private ExecutorService threadPool;
  private CountDownLatch inControlQueueOfferLatch;
  private CountDownLatch inStreamQueueOfferLatch;

  static void close(Closeable closeable) {
    if (closeable != null) {
      try {
        closeable.close();
      } catch (IOException ignored) {
      }
    }
  }

  static SocketChannel acceptSocketChannel(
      ServerSocketChannel serverSocketChannel) throws IOException {
    SocketChannel socketChannel = serverSocketChannel.accept();
    configure(socketChannel);
    socketChannel.configureBlocking(false);
    close(serverSocketChannel);
    return socketChannel;
  }

  private static void configure(SocketChannel socketChannel) throws SocketException {
    socketChannel.socket().setTcpNoDelay(true);
  }

  @Before
  public void before() throws Exception {
    inControlQueueOfferLatch = new CountDownLatch(1);
    inStreamQueueOfferLatch = new CountDownLatch(1);
    communicatorTester = new CommunicatorTester(inControlQueueOfferLatch, inStreamQueueOfferLatch);
    gatewayMetrics = new GatewayMetrics();
    threadPool = Executors.newSingleThreadExecutor();

    // Get an available port
    serverPort = SysUtils.getFreePort();
  }

  @After
  public void after() throws NoSuchFieldException, IllegalAccessException {
    if (communicatorTester != null) {
      communicatorTester.stop();
    }

    if (streamManagerClient != null) {
      streamManagerClient.stop();
    }

    if (threadPool != null) {
      threadPool.shutdownNow();
    }
  }

  protected int getServerPort() {
    return serverPort;
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

  protected Communicator<Message> getInStreamQueue() {
    return communicatorTester.getInStreamQueue();
  }

  public CountDownLatch getInControlQueueOfferLatch() {
    return inControlQueueOfferLatch;
  }

  public CountDownLatch getInStreamQueueOfferLatch() {
    return inStreamQueueOfferLatch;
  }

  IncomingPacket readIncomingPacket(SocketChannel socketChannel) throws IOException {
    // Receive request
    IncomingPacket incomingPacket = new IncomingPacket();

    Selector readSelector = Selector.open();
    socketChannel.register(readSelector, SelectionKey.OP_READ);
    readSelector.select(HeronServerTester.RESPONSE_RECEIVED_TIMEOUT.toMillis());

    // reading might not return the full payload in one shot. It could take 2 due to the header
    // and the data read
    if (incomingPacket.readFromChannel(socketChannel) != 0) {
      readSelector.select(HeronServerTester.RESPONSE_RECEIVED_TIMEOUT.toMillis());
      assertEquals(0, incomingPacket.readFromChannel(socketChannel));
    }

    // Though we do not use typeName, we need to unpack it first, since the order is required.
    // doing this as a convenience since none of the callers of this method need this.
    incomingPacket.unpackString();
    return incomingPacket;
  }

  StreamManagerClient runStreamManagerClient() {
    SystemConfig systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    HeronSocketOptions socketOptions = new HeronSocketOptions(
         systemConfig.getInstanceNetworkWriteBatchSize(),
         systemConfig.getInstanceNetworkWriteBatchTime(),
         systemConfig.getInstanceNetworkReadBatchSize(),
         systemConfig.getInstanceNetworkReadBatchTime(),
         systemConfig.getInstanceNetworkOptionsSocketSendBufferSize(),
         systemConfig.getInstanceNetworkOptionsSocketReceivedBufferSize(),
         systemConfig.getInstanceNetworkOptionsMaximumPacketSize());

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
