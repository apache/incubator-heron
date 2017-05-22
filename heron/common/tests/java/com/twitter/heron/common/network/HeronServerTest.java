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

package com.twitter.heron.common.network;

import java.nio.channels.SelectableChannel;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.proto.testing.Tests;

/**
 * HeronServer Tester.
 */
public class HeronServerTest {
  private static final int N = 10;
  private static final int WAIT_TIME_MS = 2 * 1000;
  private static final String MESSAGE = "message";
  private static final Logger LOG = Logger.getLogger(HeronServerTest.class.getName());
  private static final String SERVER_HOST = "127.0.0.1";
  private static final HeronSocketOptions TEST_SOCKET_OPTIONS = new HeronSocketOptions(
      ByteAmount.fromMegabytes(100), Duration.ofMillis(100),
      ByteAmount.fromMegabytes(100), Duration.ofMillis(100),
      ByteAmount.fromMegabytes(5),
      ByteAmount.fromMegabytes(5));
  // Following are state variable to test correctness
  private volatile boolean isOnConnectedInvoked = false;
  private volatile boolean isOnRequestInvoked = false;
  private volatile boolean isOnMessageInvoked = false;
  private volatile boolean isOnCloseInvoked = false;
  private volatile boolean isTimerEventInvoked = false;
  private volatile boolean isClientReceivedResponse = false;
  private HeronServer heronServer;
  private NIOLooper serverLooper;
  private HeronClient heronClient;
  private NIOLooper clientLooper;
  private ExecutorService threadsPool;
  // Control whether we need to send request & response
  private volatile boolean isRequestNeed = false;
  private volatile boolean isMessageNeed = false;
  private volatile int messagesReceieved = 0;

  /**
   * JUnit rule for expected exception
   */
  @Rule
  public final ExpectedException exception = ExpectedException.none();

  @Before
  public void before() throws Exception {
    // Get an available port
    int serverPort = SysUtils.getFreePort();

    serverLooper = new NIOLooper();
    heronServer = new SimpleHeronServer(serverLooper, SERVER_HOST, serverPort);

    clientLooper = new NIOLooper();
    heronClient = new SimpleHeronClient(clientLooper, SERVER_HOST, serverPort);

    threadsPool = Executors.newFixedThreadPool(2);
  }

  @After
  public void after() throws Exception {
    threadsPool.shutdownNow();

    heronServer.stop();
    heronServer = null;

    heronClient.stop();
    heronClient = null;

    serverLooper.exitLoop();
    serverLooper = null;
    clientLooper.exitLoop();
    clientLooper = null;

    threadsPool = null;

    // Reset the state
    isOnConnectedInvoked = false;
    isOnRequestInvoked = false;
    isOnMessageInvoked = false;
    isOnCloseInvoked = false;
    isTimerEventInvoked = false;
    isClientReceivedResponse = false;
  }

  /**
   * Method: registerOnMessage(Message.Builder builder)
   */
  @Test
  public void testRegisterOnMessage() throws Exception {
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
  public void testRegisterOnRequest() throws Exception {
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
  public void testStart() throws Exception {
    Assert.assertTrue(heronServer.start());
  }

  /**
   * Method: stop()
   */
  @Test
  public void testClose() throws Exception {
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
  public void testHandleAccept() throws Exception {
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
  public void testHandleRead() throws Exception {
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
  public void testHandleWrite() throws Exception {
    runBase();

    Map<SocketChannel, SocketChannelHelper> activeConnections = heronServer.getActiveConnections();
    // Exceptions should not be thrown in this call
    heronServer.handleWrite(activeConnections.keySet().iterator().next());
  }

  /**
   * Method: handleConnect(SelectableChannel channel)
   */
  @Test
  public void testHandleConnect() throws Exception {
    exception.expect(RuntimeException.class);
    heronServer.handleConnect(null);
  }

  /**
   * Method: handleError(SelectableChannel channel)
   */
  @Test
  public void testHandleError() throws Exception {
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
  public void testGetNIOLooper() throws Exception {
    Assert.assertNotNull(heronServer.getNIOLooper());
  }

  /**
   * Method: registerTimerEventInSeconds(long timerInSeconds, Runnable task)
   */
  @Test
  public void testRegisterTimerEventInSeconds() throws Exception {
    Runnable r = new Runnable() {
      @Override
      public void run() {
        isTimerEventInvoked = true;
      }
    };
    heronServer.registerTimerEvent(Duration.ofSeconds(1), r);

    runBase();

    Assert.assertTrue(isTimerEventInvoked);
  }

  /**
   * Method: sendResponse(REQID rid, SocketChannel channel, Message response)
   */
  @Test
  public void testSendResponse() throws Exception {
    isRequestNeed = true;

    runBase();

    Assert.assertTrue(isOnRequestInvoked);
    Assert.assertTrue(isClientReceivedResponse);

    isRequestNeed = false;
  }

  /**
   * Method: sendMessage(SocketChannel channel, Message message)
   */
  @Test
  public void testSendMessage() throws Exception {
    isRequestNeed = true;
    isMessageNeed = true;

    runBase();

    Assert.assertTrue(isOnMessageInvoked);
    Assert.assertEquals(N, messagesReceieved);

    isRequestNeed = false;
    isMessageNeed = false;
    messagesReceieved = 0;
  }

  /**
   * Method: registerTimerEventInNanoSeconds(long timerInNanoSecnods, Runnable task)
   */
  @Test
  public void testRegisterTimerEventInNanoSeconds() throws Exception {
    Runnable r = new Runnable() {
      @Override
      public void run() {
        isTimerEventInvoked = true;
      }
    };
    heronServer.registerTimerEvent(Duration.ZERO, r);

    runBase();

    Assert.assertTrue(isTimerEventInvoked);
  }

  /**
   * Method: onConnect(SocketChannel channel)
   */
  @Test
  public void testOnConnect() throws Exception {
    runBase();

    Map<SocketChannel, SocketChannelHelper> activeConnections = heronServer.getActiveConnections();
    heronServer.onConnect(activeConnections.keySet().iterator().next());

    Assert.assertTrue(isOnConnectedInvoked);
  }

  /**
   * Method: onRequest(REQID rid, SocketChannel channel, Message request)
   */
  @Test
  public void testOnRequest() throws Exception {
    runBase();

    Map<SocketChannel, SocketChannelHelper> activeConnections = heronServer.getActiveConnections();
    heronServer.onRequest(REQID.generate(), activeConnections.keySet().iterator().next(), null);

    Assert.assertTrue(isOnRequestInvoked);
  }

  /**
   * Method: onMessage(SocketChannel channel, Message message)
   */
  @Test
  public void testOnMessage() throws Exception {
    runBase();

    Map<SocketChannel, SocketChannelHelper> activeConnections = heronServer.getActiveConnections();
    heronServer.onMessage(activeConnections.keySet().iterator().next(), null);

    Assert.assertTrue(isOnMessageInvoked);
  }

  /**
   * Method: onClose(SocketChannel channel)
   */
  @Test
  public void testOnClose() throws Exception {
    runBase();

    Map<SocketChannel, SocketChannelHelper> activeConnections = heronServer.getActiveConnections();
    heronServer.onClose(activeConnections.keySet().iterator().next());

    Assert.assertTrue(isOnCloseInvoked);
  }

  private void runServer() {
    Runnable runServer = new Runnable() {
      @Override
      public void run() {
        heronServer.start();
        heronServer.getNIOLooper().loop();
      }
    };
    threadsPool.execute(runServer);
  }

  private void runClient() {
    Runnable runClient = new Runnable() {
      @Override
      public void run() {
        heronClient.start();
        heronClient.getNIOLooper().loop();
      }
    };
    threadsPool.execute(runClient);
  }

  private void runBase() throws Exception {
    // First run Server
    runServer();

    // Wait a while for server fully starting
    Thread.sleep(WAIT_TIME_MS);

    // Then run Client
    runClient();

    // Should be connected
    Thread.sleep(WAIT_TIME_MS);
  }

  private class SimpleHeronServer extends HeronServer {

    SimpleHeronServer(NIOLooper s, String host, int port) {
      super(s, host, port, TEST_SOCKET_OPTIONS);
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
    }

    @Override
    public void onRequest(REQID rid, SocketChannel channel, Message request) {
      isOnRequestInvoked = true;

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
        messagesReceieved++;
        Assert.assertEquals(MESSAGE, ((Tests.EchoServerResponse) message).getEchoResponse());
      } else {
        Assert.fail("Unknown message received");
      }
    }

    @Override
    public void onClose(SocketChannel socketChannel) {
      isOnCloseInvoked = true;
    }
  }

  private class SimpleHeronClient extends HeronClient {
    SimpleHeronClient(NIOLooper looper, String host, int port) {
      super(looper, host, port, TEST_SOCKET_OPTIONS);
    }

    @Override
    public void onError() {

    }

    @Override
    public void onClose() {

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
          for (int i = 0; i < N; i++) {
            sendMessage(message.build());
          }
        }
      } else {
        Assert.fail("Unknown type of response received");
      }
    }

    @Override
    public void onIncomingMessage(Message message) {
      LOG.info("OnIncoming Message: " + message);
    }
  }
}
