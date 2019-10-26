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

package org.apache.heron.common.test;

import java.io.IOException;
import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.common.network.HeronClient;
import org.apache.heron.common.network.HeronServer;
import org.apache.heron.common.network.REQID;
import org.apache.heron.common.network.SocketChannelHelper;
import org.apache.heron.common.network.StatusCode;
import org.apache.heron.common.testhelpers.HeronServerTester;
import org.apache.heron.proto.testing.Tests;

/**
 * HeronServer Tester.
 */
public class HeronServerTest {
  private static final int TOTAL_MESSAGES = 10;
  private static final String MESSAGE = "message";
  private static final Logger LOG = Logger.getLogger(HeronServerTest.class.getName());
  // Following are state variable to test correctness
  private volatile boolean isOnConnectedInvoked = false;
  private volatile boolean isOnRequestInvoked = false;
  private volatile boolean isOnMessageInvoked = false;
  private volatile boolean isOnCloseInvoked = false;
  private volatile boolean isTimerEventInvoked = false;
  private volatile boolean isClientReceivedResponse = false;

  private HeronServer heronServer;
  private HeronClient heronClient;

  // Control whether we need to send request & response
  private volatile boolean isRequestNeed = false;
  private volatile boolean isMessageNeed = false;
  private volatile int messagesReceived = 0;

  private CountDownLatch serverOnConnectSignal;
  private CountDownLatch serverOnMessageSignal;
  private CountDownLatch serverOnRequestSignal;
  private CountDownLatch clientOnConnectSignal;
  private CountDownLatch clientOnResponseSignal;
  private HeronServerTester heronServerTester;

  /**
   * JUnit rule for expected exception
   */
  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Before
  public void before() throws IOException {
    // Get an available port
    int serverPort = SysUtils.getFreePort();

    serverOnConnectSignal = new CountDownLatch(1);
    serverOnMessageSignal = new CountDownLatch(TOTAL_MESSAGES);
    serverOnRequestSignal = new CountDownLatch(1);
    clientOnConnectSignal = new CountDownLatch(1);
    clientOnResponseSignal = new CountDownLatch(1);
    heronServer = new SimpleHeronServer(new NIOLooper(), HeronServerTester.SERVER_HOST, serverPort,
        serverOnConnectSignal, serverOnMessageSignal, serverOnRequestSignal);
    heronClient = new SimpleHeronClient(new NIOLooper(), HeronServerTester.SERVER_HOST, serverPort,
        clientOnConnectSignal, clientOnResponseSignal);

    heronServerTester = new HeronServerTester(heronServer, heronClient);
  }

  @After
  public void after() {
    heronServerTester.stop();
  }

  /**
   * Method: registerOnMessage(Message.Builder builder)
   */
  @Test
  public void testRegisterOnMessage() {
    Message.Builder m = Tests.EchoServerResponse.newBuilder();
    heronServer.registerOnMessage(m);
    for (Map.Entry<String, Message.Builder> message : heronServer.getMessageMap().entrySet()) {
      Assert.assertEquals("heron.proto.testing.EchoServerResponse", message.getKey());
      Assert.assertEquals(m, message.getValue());
    }
  }

  /**
   * Method: registerOnRequest(Message.Builder builder)
   */
  @Test
  public void testRegisterOnRequest() {
    Message.Builder r = Tests.EchoServerRequest.newBuilder();
    heronServer.registerOnRequest(r);
    for (Map.Entry<String, Message.Builder> request : heronServer.getRequestMap().entrySet()) {
      Assert.assertEquals("heron.proto.testing.EchoServerRequest", request.getKey());
      Assert.assertEquals(r, request.getValue());
    }
  }

  /**
   * Method: start()
   */
  @Test
  public void testStart() {
    Assert.assertTrue(heronServer.start());
  }

  /**
   * Method: stop()
   */
  @Test
  public void testClose() {
    runBase();

    heronServer.stop();

    Map<SocketChannel, SocketChannelHelper> activeConnections = heronServer.getActiveConnections();
    ServerSocketChannel acceptChannel = heronServer.getAcceptChannel();

    Assert.assertNotNull(acceptChannel);
    Assert.assertTrue(!acceptChannel.isOpen());
    Assert.assertNotNull(activeConnections);
    Assert.assertEquals(0, activeConnections.size());
  }

  /**
   * Method: handleAccept(SelectableChannel channel)
   */
  @Test
  public void testHandleAccept() {
    runBase();

    Map<SocketChannel, SocketChannelHelper> activeConnections = heronServer.getActiveConnections();
    ServerSocketChannel acceptChannel = heronServer.getAcceptChannel();

    Assert.assertNotNull(acceptChannel);
    Assert.assertTrue(acceptChannel.isOpen());
    Assert.assertNotNull(activeConnections);
    Assert.assertEquals(1, activeConnections.size());
  }

  /**
   * Method: handleRead(SelectableChannel channel)
   */
  @Test
  public void testHandleRead() {
    runBase();

    Map<SocketChannel, SocketChannelHelper> activeConnections = heronServer.getActiveConnections();
    // Exceptions should not be thrown in this call
    if (activeConnections.size() != 0) {
      // No errors happened
      heronServer.handleRead(activeConnections.keySet().iterator().next());
    }
  }

  /**
   * Method: handleWrite(SelectableChannel channel)
   */
  @Test
  public void testHandleWrite() {
    runBase();

    Map<SocketChannel, SocketChannelHelper> activeConnections = heronServer.getActiveConnections();
    // Exceptions should not be thrown in this call
    heronServer.handleWrite(activeConnections.keySet().iterator().next());
  }

  /**
   * Method: handleConnect(SelectableChannel channel)
   */
  @Test
  public void testHandleConnect() {
    exception.expect(RuntimeException.class);
    heronServer.handleConnect(null);
  }

  /**
   * Method: handleError(SelectableChannel channel)
   */
  @Test
  public void testHandleError() {
    runBase();

    Map<SocketChannel, SocketChannelHelper> activeConnections = heronServer.getActiveConnections();
    SelectableChannel channel = activeConnections.keySet().iterator().next();
    heronServer.handleError(channel);

    Assert.assertEquals(0, activeConnections.size());
  }

  /**
   * Method: getNIOLooper()
   */
  @Test
  public void testGetNIOLooper() {
    Assert.assertNotNull(heronServer.getNIOLooper());
  }

  /**
   * Method: registerTimerEventInSeconds(long timerInSeconds, Runnable task)
   */
  @Test
  public void testRegisterTimerEventInSeconds() {
    final CountDownLatch timerEventLatch = new CountDownLatch(1);
    Runnable r = new Runnable() {
      @Override
      public void run() {
        isTimerEventInvoked = true;
        timerEventLatch.countDown();
      }
    };
    heronServer.registerTimerEvent(Duration.ofSeconds(1), r);

    runBase();
    HeronServerTester.await(timerEventLatch);

    Assert.assertTrue(isTimerEventInvoked);
  }

  /**
   * Method: sendResponse(REQID rid, SocketChannel channel, Message response)
   */
  @Test
  public void testSendResponse() {
    isRequestNeed = true;

    runBase();
    HeronServerTester.await(serverOnRequestSignal);
    HeronServerTester.await(clientOnResponseSignal);

    Assert.assertTrue(isOnRequestInvoked);
    Assert.assertTrue(isClientReceivedResponse);

    isRequestNeed = false;
  }

  /**
   * Method: sendMessage(SocketChannel channel, Message message)
   */
  @Test
  public void testSendMessage() {
    isRequestNeed = true;
    isMessageNeed = true;

    runBase();
    HeronServerTester.await(serverOnMessageSignal);

    Assert.assertTrue(isOnMessageInvoked);
    Assert.assertEquals(TOTAL_MESSAGES, messagesReceived);

    isRequestNeed = false;
    isMessageNeed = false;
    messagesReceived = 0;
  }

  /**
   * Method: registerTimerEventInNanoSeconds(long timerInNanoSecnods, Runnable task)
   */
  @Test
  public void testRegisterTimerEventInNanoSeconds() {
    final CountDownLatch runnableLatch = new CountDownLatch(1);
    Runnable r = new Runnable() {
      @Override
      public void run() {
        isTimerEventInvoked = true;
        runnableLatch.countDown();
      }
    };
    heronServer.registerTimerEvent(Duration.ZERO, r);

    runBase();

    HeronServerTester.await(runnableLatch);
    Assert.assertTrue(isTimerEventInvoked);
  }

  /**
   * Method: onConnect(SocketChannel channel)
   */
  @Test
  public void testOnConnect() {
    runBase();

    Map<SocketChannel, SocketChannelHelper> activeConnections = heronServer.getActiveConnections();
    heronServer.onConnect(activeConnections.keySet().iterator().next());

    Assert.assertTrue(isOnConnectedInvoked);
  }

  /**
   * Method: onRequest(REQID rid, SocketChannel channel, Message request)
   */
  @Test
  public void testOnRequest() {
    runBase();

    Map<SocketChannel, SocketChannelHelper> activeConnections = heronServer.getActiveConnections();
    heronServer.onRequest(REQID.generate(), activeConnections.keySet().iterator().next(), null);

    Assert.assertTrue(isOnRequestInvoked);
  }

  /**
   * Method: onMessage(SocketChannel channel, Message message)
   */
  @Test
  public void testOnMessage() {
    runBase();

    Map<SocketChannel, SocketChannelHelper> activeConnections = heronServer.getActiveConnections();
    heronServer.onMessage(activeConnections.keySet().iterator().next(), null);

    Assert.assertTrue(isOnMessageInvoked);
  }

  /**
   * Method: onClose(SocketChannel channel)
   */
  @Test
  public void testOnClose() {
    runBase();

    Map<SocketChannel, SocketChannelHelper> activeConnections = heronServer.getActiveConnections();
    heronServer.onClose(activeConnections.keySet().iterator().next());

    Assert.assertTrue(isOnCloseInvoked);
  }

  private void runClient() {
    Runnable runClient = new Runnable() {
      @Override
      public void run() {
        heronClient.start();
        heronClient.getNIOLooper().loop();
      }
    };
  }

  private void runBase() {
    heronServerTester.start();
    HeronServerTester.await(clientOnConnectSignal);
    HeronServerTester.await(serverOnConnectSignal);
  }

  private class SimpleHeronServer extends HeronServer {
    private final CountDownLatch onConnectSignal;
    private final CountDownLatch onMessageSignal;
    private final CountDownLatch onRequestSignal;

    SimpleHeronServer(NIOLooper s, String host, int port, CountDownLatch onConnectSignal,
                      CountDownLatch onMessageSignal, CountDownLatch onRequestSignal) {
      super(s, host, port, HeronServerTester.TEST_SOCKET_OPTIONS);
      this.onConnectSignal = onConnectSignal;
      this.onMessageSignal = onMessageSignal;
      this.onRequestSignal = onRequestSignal;
    }

    @Override
    public void onConnect(SocketChannel socketChannel) {
      LOG.info("Server got a new connection from host:port:"
          + socketChannel.socket().getRemoteSocketAddress());
      isOnConnectedInvoked = true;

      // We only register request when we need to test on sendResponse or sendMessage
      if (isRequestNeed) {
        registerOnRequest(Tests.EchoServerRequest.newBuilder());
      }

      // If We need to test sendMessage, we would registerOnMessage EchoServerResponse
      if (isMessageNeed) {
        registerOnMessage(Tests.EchoServerResponse.newBuilder());
      }
      onConnectSignal.countDown();
    }

    @Override
    public void onRequest(REQID rid, SocketChannel channel, Message request) {
      isOnRequestInvoked = true;
      onRequestSignal.countDown();

      if (request == null) {
        // We just want to see whether we could invoke onRequest() normally
        return;
      }

      if (request instanceof Tests.EchoServerRequest) {
        Tests.EchoServerResponse.Builder response = Tests.EchoServerResponse.newBuilder();
        Tests.EchoServerRequest req = (Tests.EchoServerRequest) request;
        response.setEchoResponse(req.getEchoRequest());

        // We only send back response when we need to test on sendResponse or sendMessage
        if (isRequestNeed) {
          sendResponse(rid, channel, response.build());
        }
      } else {
        LOG.info("Type of request: " + request);
        throw new RuntimeException("Unknown type of request received");
      }
    }

    @Override
    public void onMessage(SocketChannel socketChannel, Message message) {
      isOnMessageInvoked = true;

      if (message == null) {
        // We just want to see whether we could invoke onMessage() normally
        return;
      }

      if (message instanceof Tests.EchoServerResponse) {
        messagesReceived++;
        Assert.assertEquals(MESSAGE, ((Tests.EchoServerResponse) message).getEchoResponse());
      } else {
        Assert.fail("Unknown message received");
      }
      onMessageSignal.countDown();
    }

    @Override
    public void onClose(SocketChannel socketChannel) {
      isOnCloseInvoked = true;
    }
  }

  private class SimpleHeronClient extends HeronServerTester.AbstractTestClient {
    private final CountDownLatch onConnectSignal;
    private final CountDownLatch onResponseSignal;

    SimpleHeronClient(NIOLooper looper, String host, int port,
                      CountDownLatch onConnectSignal, CountDownLatch onResponseSignal) {
      super(looper, host, port, HeronServerTester.TEST_SOCKET_OPTIONS);
      this.onConnectSignal = onConnectSignal;
      this.onResponseSignal = onResponseSignal;
    }

    @Override
    public void onConnect(StatusCode statusCode) {
      if (statusCode != StatusCode.OK) {
        Assert.fail("Connection with server failed");
      } else {
        LOG.info("Connected with server");

        // We only send request when we need to test on sendResponse or sendMessage
        if (isRequestNeed) {
          sendRequest();
        }
      }
      onConnectSignal.countDown();
    }

    private void sendRequest() {
      Tests.EchoServerRequest.Builder r = Tests.EchoServerRequest.newBuilder();
      r.setEchoRequest("Dummy");
      sendRequest(r.build(), Tests.EchoServerResponse.newBuilder());
    }

    @Override
    public void onResponse(StatusCode statusCode, Object o, Message response) {
      if (response instanceof Tests.EchoServerResponse) {
        Tests.EchoServerResponse r = (Tests.EchoServerResponse) response;
        isClientReceivedResponse = true;
        Assert.assertEquals(r.getEchoResponse(), "Dummy");

        // If we want to test sendMessage, we would send Message
        if (isMessageNeed) {
          Tests.EchoServerResponse.Builder message =
              Tests.EchoServerResponse.newBuilder().setEchoResponse(MESSAGE);
          for (int i = 0; i < TOTAL_MESSAGES; i++) {
            sendMessage(message.build());
          }
        }
      } else {
        Assert.fail("Unknown type of response received");
      }
      onResponseSignal.countDown();
    }
  }
}
