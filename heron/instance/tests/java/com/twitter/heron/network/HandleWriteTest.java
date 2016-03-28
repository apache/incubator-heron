package com.twitter.heron.network;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.config.SystemConfig;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SingletonRegistry;
import com.twitter.heron.common.basics.SlaveLooper;
import com.twitter.heron.common.basics.WakeableLooper;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.network.IncomingPacket;
import com.twitter.heron.common.network.OutgoingPacket;
import com.twitter.heron.common.network.REQID;
import com.twitter.heron.instance.InstanceControlMsg;
import com.twitter.heron.metrics.GatewayMetrics;
import com.twitter.heron.proto.stmgr.StreamManager;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.resource.Constants;
import com.twitter.heron.resource.UnitTestHelper;

/**
 * To test whether Instance's handleWrite() from the stream manager.
 * 1. The instance will connect to stream manager successfully
 * 2. We will construct a bunch of Mock Message and offer them to outStreamQueue.
 * 3. The instance should get items from outStreamQueue and send them to Stream Manager.
 * 4. Check whether items received in Stream Manager match the Mock Message we constructed in Instance.
 */

public class HandleWriteTest {
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

    streamManagerClient.stop();
    streamManagerClient = null;

    nioLooper.exitLoop();
    nioLooper = null;
    slaveLooper = null;
    inStreamQueue = null;
    outStreamQueue = null;

    gatewayMetrics = null;

    threadsPool.shutdownNow();
    threadsPool = null;
  }

  @Test
  public void testHandleWrite() throws Exception {
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
          break;
        } else {
          SysUtils.sleep(Constants.RETRY_INTERVAL_MS);
        }
      }

      for (int i = 0; i < 10; i++) {
        // We randomly choose some messages writing to stream mgr
        streamManagerClient.sendMessage(UnitTestHelper.getRegisterInstanceResponse());
      }

      for (int i = 0; i < 10; i++) {
        incomingPacket = new IncomingPacket();
        while (incomingPacket.readFromChannel(socketChannel) != 0) {

        }
        typeName = incomingPacket.unpackString();
        rid = incomingPacket.unpackREQID();
        StreamManager.RegisterInstanceResponse.Builder builder
            = StreamManager.RegisterInstanceResponse.newBuilder();
        incomingPacket.unpackMessage(builder);
        StreamManager.RegisterInstanceResponse response = builder.build();

        Assert.assertNotNull(response);
        Assert.assertTrue(response.isInitialized());
        Assert.assertEquals(Common.StatusCode.OK, response.getStatus().getStatus());
        Assert.assertEquals(1, response.getPplan().getStmgrsCount());
        Assert.assertEquals(2, response.getPplan().getInstancesCount());
        Assert.assertEquals(1, response.getPplan().getTopology().getBoltsCount());
        Assert.assertEquals(1, response.getPplan().getTopology().getSpoutsCount());
        Assert.assertEquals(TopologyAPI.TopologyState.RUNNING, response.getPplan().getTopology().getState());
      }

      nioLooper.exitLoop();

    } catch (ClosedByInterruptException ignored) {
    } catch (ClosedChannelException ignored) {
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      close(socketChannel);
    }
  }

  static void close(Closeable sc2) {
    if (sc2 != null) try {
      sc2.close();
    } catch (IOException ignored) {
    }
  }

  static void configure(SocketChannel sc) throws SocketException {
    sc.socket().setTcpNoDelay(true);
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
              systemConfig.getInstanceNetworkWriteBatchSizeBytes(),
              systemConfig.getInstanceNetworkWriteBatchTimeMs(),
              systemConfig.getInstanceNetworkReadBatchSizeBytes(),
              systemConfig.getInstanceNetworkReadBatchTimeMs(),
              systemConfig.getInstanceNetworkOptionsSocketSendBufferSizeBytes(),
              systemConfig.getInstanceNetworkOptionsSocketReceivedBufferSizeBytes()
          );

          streamManagerClient = new StreamManagerClient(nioLooper, HOST, serverPort,
              "topology-name", "topologyId", UnitTestHelper.getInstance("bolt-id"),
              inStreamQueue, outStreamQueue, inControlQueue, socketOptions, gatewayMetrics);
          streamManagerClient.start();
          nioLooper.loop();
        } catch (Exception ignored) {

        }
      }
    };
    threadsPool.execute(r);
  }
}
