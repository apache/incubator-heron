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

package com.twitter.heron.network;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.basics.WakeableLooper;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.network.IncomingPacket;
import com.twitter.heron.common.network.OutgoingPacket;
import com.twitter.heron.common.network.REQID;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.instance.InstanceControlMsg;
import com.twitter.heron.metrics.GatewayMetrics;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.resource.Constants;
import com.twitter.heron.resource.UnitTestHelper;

/**
 * To test whether Instance could connect to stream manager successfully.
 * It will have a mock stream manager, which will:
 * 1. Open a socket, and waiting for the RegisterInstanceRequest constructed by us
 * 2. Once receiving a RegisterInstanceRequest, check whether the RegisterInstanceRequest's info matches
 * the one we constructed.
 * 3. Send back a mock RegisterInstanceResponse with Physical Plan.
 * 4. Check whether the Instance adds the Physical Plan to the singletonRegistry.
 */

public class ConnectTest {
  private static final String HOST = "127.0.0.1";
  private static int serverPort;

  // Only one outStreamQueue, which is responsible for both control tuples and data tuples
  private Communicator<HeronTuples.HeronTupleSet> outStreamQueue;

  // This blocking queue is used to buffer tuples read from socket and ready to be used by instance
  // For spout, it will buffer Control tuple, while for bolt, it will buffer data tuple.
  private Communicator<HeronTuples.HeronTupleSet> inStreamQueue;

  private Communicator<InstanceControlMsg> inControlQueue;

  private NIOLooper nioLooper;
  private WakeableLooper slaveLooper;

  private StreamManagerClient streamManagerClient;

  private GatewayMetrics gatewayMetrics;

  private ExecutorService threadsPool;

  @BeforeClass
  public static void beforeClass() throws Exception {

  }

  @AfterClass
  public static void afterClass() throws Exception {

  }

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
    UnitTestHelper.addSystemConfigToSingleton();

    nioLooper = new NIOLooper();
    slaveLooper = new SlaveLooper();
    inStreamQueue = new Communicator<HeronTuples.HeronTupleSet>(nioLooper, slaveLooper);
    inStreamQueue.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    outStreamQueue = new Communicator<HeronTuples.HeronTupleSet>(slaveLooper, nioLooper);
    outStreamQueue.init(Constants.QUEUE_BUFFER_SIZE, Constants.QUEUE_BUFFER_SIZE, 0.5);
    inControlQueue = new Communicator<InstanceControlMsg>(nioLooper, slaveLooper);

    gatewayMetrics = new GatewayMetrics();

    threadsPool = Executors.newSingleThreadExecutor();

    // Get an available port
    serverPort = SysUtils.getFreePort();
  }

  @After
  public void after() throws Exception {
    UnitTestHelper.clearSingletonRegistry();

    if (streamManagerClient != null) {
      streamManagerClient.stop();
      streamManagerClient = null;
    }

    if (nioLooper != null) {
      nioLooper.exitLoop();
      nioLooper = null;
    }
    slaveLooper = null;
    inStreamQueue = null;
    outStreamQueue = null;

    gatewayMetrics = null;

    if (threadsPool != null) {
      threadsPool.shutdownNow();
      threadsPool = null;
    }
  }

  /**
   * Test connection
   */
  @Test
  public void testStart() throws Exception {
    ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
    serverSocketChannel.socket().bind(new InetSocketAddress(HOST, serverPort));

    SocketChannel socketChannel = null;
    try {
      runStreamManagerClient();

      socketChannel = serverSocketChannel.accept();
      configure(socketChannel);
      socketChannel.configureBlocking(false);
      close(serverSocketChannel);

      // Receive request
      IncomingPacket incomingPacket = new IncomingPacket();
      while (incomingPacket.readFromChannel(socketChannel) != 0) {
        // 1ms sleep to mitigate busy looping
        SysUtils.sleep(Duration.ofMillis(1));
      }

      // Send back response
      // Though we do not use typeName, we need to unpack it first,
      // since the order is required
      String typeName = incomingPacket.unpackString();
      REQID rid = incomingPacket.unpackREQID();

      OutgoingPacket outgoingPacket
          = new OutgoingPacket(rid, UnitTestHelper.getRegisterInstanceResponse());
      outgoingPacket.writeToChannel(socketChannel);

      for (int i = 0; i < Constants.RETRY_TIMES; i++) {
        InstanceControlMsg instanceControlMsg = inControlQueue.poll();
        if (instanceControlMsg != null) {
          nioLooper.exitLoop();
          threadsPool.shutdownNow();

          PhysicalPlanHelper physicalPlanHelper = instanceControlMsg.getNewPhysicalPlanHelper();

          Assert.assertEquals("test-bolt", physicalPlanHelper.getMyComponent());
          Assert.assertEquals(InetAddress.getLocalHost().getHostName(),
              physicalPlanHelper.getMyHostname());
          Assert.assertEquals(0, physicalPlanHelper.getMyInstanceIndex());
          Assert.assertEquals(1, physicalPlanHelper.getMyTaskId());

          break;
        } else {
          SysUtils.sleep(Constants.RETRY_INTERVAL);
        }
      }

    } catch (ClosedChannelException ignored) {
    } finally {
      close(socketChannel);
    }
  }

  void runStreamManagerClient() {
    Runnable r = new Runnable() {
      @Override
      public void run() {
        try {
          SystemConfig systemConfig =
              (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(
                  SystemConfig.HERON_SYSTEM_CONFIG);

          HeronSocketOptions socketOptions = new HeronSocketOptions(
              systemConfig.getInstanceNetworkWriteBatchSize(),
              systemConfig.getInstanceNetworkWriteBatchTime(),
              systemConfig.getInstanceNetworkReadBatchSize(),
              systemConfig.getInstanceNetworkReadBatchTime(),
              systemConfig.getInstanceNetworkOptionsSocketSendBufferSize(),
              systemConfig.getInstanceNetworkOptionsSocketReceivedBufferSize()
          );

          streamManagerClient = new StreamManagerClient(nioLooper, HOST, serverPort,
              "topology-name", "topologyId", UnitTestHelper.getInstance("bolt-id"),
              inStreamQueue, outStreamQueue, inControlQueue, socketOptions, gatewayMetrics);
          streamManagerClient.start();
          nioLooper.loop();
        } finally {
          streamManagerClient.stop();
          nioLooper.exitLoop();
        }
      }
    };
    threadsPool.execute(r);
  }
}
