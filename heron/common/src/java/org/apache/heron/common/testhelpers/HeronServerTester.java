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

package org.apache.heron.common.testhelpers;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.Message;

import org.apache.heron.common.basics.ByteAmount;
import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.network.HeronClient;
import org.apache.heron.common.network.HeronServer;
import org.apache.heron.common.network.HeronSocketOptions;
import org.apache.heron.common.network.StatusCode;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Class to help simplify testing HeronServer implementations by consolidating common test
 * functionality. A new instance of this class should be used for each test. After initializing the
 * class, tests should call start() which guarantees the following:
 * <ol>
 * <li>the server has started</li>
 * <li>the client has been launched</li>
 * </ol>
 * Upon test completion, the stop() method should be closed to properly release resources. This
 * method is safe to be called on an instance that was never properly started.
 */
public class HeronServerTester {
  public static final String SERVER_HOST = "127.0.0.1";
  public static final HeronSocketOptions TEST_SOCKET_OPTIONS = new HeronSocketOptions(
      ByteAmount.fromMegabytes(100), Duration.ofMillis(100),
      ByteAmount.fromMegabytes(100), Duration.ofMillis(100),
      ByteAmount.fromMegabytes(5),
      ByteAmount.fromMegabytes(5),
      ByteAmount.fromMegabytes(10));
  private static final Duration DEFAULT_LATCH_TIMEOUT = Duration.ofSeconds(2);
  private static final Duration SERVER_START_TIMEOUT = Duration.ofSeconds(2);
  public static final Duration RESPONSE_RECEIVED_TIMEOUT = Duration.ofSeconds(4);

  private final HeronServer server;
  private final ExecutorService threadsPool;

  private HeronClient client;
  private final CountDownLatch serverStartedSignal;
  private CountDownLatch responseReceivedSignal;
  private Duration responseReceivedTimeout;


  /**
   * Constructor to use for the common use case of sending a single request and taking some action
   * upon a response. The start() method does the following:
   * <ol>
   * <li>Initializes the server and a default client</li>
   * <li>Calls sendMessage on the client using the message from TestRequestHandler</li>
   * <li>Waits up to responseReceivedTimeout for the client to receive a response</li>
   * <li>Invokes the TestResponseHandler.handleResponse method</li>
   * </ol>
   *
   * @param server the server to test
   * @param requestHandler the request handler to use to build the request and response builder
   * @param responseHandler the handler to handle the received response
   * @param responseReceivedTimeout how long to wait for the response
   * @throws IOException
   */
  public HeronServerTester(HeronServer server,
                           TestRequestHandler requestHandler,
                           TestResponseHandler responseHandler,
                           Duration responseReceivedTimeout) throws IOException {
    this(server);
    this.responseReceivedSignal = new CountDownLatch(1);
    this.responseReceivedTimeout = responseReceivedTimeout;
    this.client = new TestClient(new NIOLooper(),
        server.getEndpoint().getHostName(), server.getEndpoint().getPort(),
        responseReceivedSignal, requestHandler, responseHandler);
  }

  /**
   * Constructor where a custom client is to be used. The start() method starts both the server and
   * the client.
   * @param server server to test
   * @param client client to test
   */
  public HeronServerTester(HeronServer server, HeronClient client) {
    this(server);
    this.client = client;
  }

  private HeronServerTester(HeronServer server) {
    this.server = server;
    this.threadsPool = Executors.newFixedThreadPool(2);
    this.serverStartedSignal = new CountDownLatch(1);
  }

  public void start() {
    // First run Server
    runServer();

    // Then run Client
    runClient();

    // Wait to make sure message was sent and response was received
    if (responseReceivedTimeout != null) {
      await(responseReceivedSignal, responseReceivedTimeout);
    }
  }

  public void stop() {
    threadsPool.shutdownNow();

    server.stop();

    if (client != null) {
      client.stop();
      client.getNIOLooper().exitLoop();
    }

    server.getNIOLooper().exitLoop();
  }

  /**
   * Convenience method to wait on a latch with a default timeout. If the timeout is reached, the
   * fail() method will be invoked.
   */
  public static void await(CountDownLatch latch) {
    await(latch, DEFAULT_LATCH_TIMEOUT);
  }

  /**
   * Convenience method to wait on a latch with a default timeout. If the timeout is reached, the
   * fail() method will be invoked.
   */
  public static void await(CountDownLatch latch, Duration timeout) {
    try {
      if (!latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
        fail(String.format(
            "Await latch failed to release until timeout of %s was reached. Check latch logic.",
            timeout));
      }
    } catch (InterruptedException e) {
      fail(String.format(
          "Await latch interrupted before timeout of %s was reached: %s",
          timeout, e));
    }
  }

  private void runServer() {
    Runnable runServer = new Runnable() {
      @Override
      public void run() {
        server.start();
        serverStartedSignal.countDown();
        server.getNIOLooper().loop();
      }
    };
    threadsPool.execute(runServer);
  }

  private void runClient() {

    Runnable runClient = new Runnable() {
      @Override
      public void run() {
        await(serverStartedSignal, SERVER_START_TIMEOUT);
        client.start();
        client.getNIOLooper().loop();
      }
    };
    threadsPool.execute(runClient);
  }

  /**
   * Abstract class to simplify writing test clients by providing default method impls.
   */
  public abstract static class AbstractTestClient extends HeronClient {
    protected AbstractTestClient(NIOLooper looper,
                                 String host, int port,
                                 HeronSocketOptions options) {
      super(looper, host, port, options);
    }

    @Override
    public void onError() {
      fail("Error in client while talking to server");
    }

    @Override
    public void onConnect(StatusCode status) {
    }

    @Override
    public void onResponse(StatusCode status, Object ctx, Message response) {
    }

    @Override
    public void onIncomingMessage(Message request) {
      fail("Incoming message not expected on the client");
    }

    @Override
    public void onClose() {
    }
  }

  private static class TestClient extends AbstractTestClient {
    private static final Logger LOG = Logger.getLogger(TestClient.class.getName());
    private final TestRequestHandler requestHandler;
    private final TestResponseHandler responseHandler;
    private final CountDownLatch responseReceivedSignal;

    TestClient(NIOLooper looper, String host, int port, CountDownLatch responseReceivedSignal,
               TestRequestHandler requestHandler, TestResponseHandler responseHandler) {
      super(looper, host, port, TEST_SOCKET_OPTIONS);
      this.requestHandler = requestHandler;
      this.responseHandler = responseHandler;
      this.responseReceivedSignal = responseReceivedSignal;
    }

    @Override
    public void onConnect(StatusCode status) {
      if (status != StatusCode.OK) {
        fail("Connection with server failed, onConnect status: " + status);
      } else {
        LOG.info("Connected with Metrics Manager Server");
        sendRequest(requestHandler.getRequestMessage(), requestHandler.getResponseBuilder());
      }
    }

    @Override
    public void onResponse(StatusCode status, Object ctx, Message response) {
      responseReceivedSignal.countDown();
      try {
        responseHandler.handleResponse(this, status, ctx, response);
        // SUPPRESS CHECKSTYLE IllegalCatch
      } catch (Exception e) {
        fail("Exception while handling response:" + e);
      }
    }
  }

  /**
   * Interface to provide the Message to be sent upon onConnect and the expected Message.Builder
   * to be used for the response.
   */
  public interface TestRequestHandler {
    Message getRequestMessage();
    Message.Builder getResponseBuilder();
  }

  /**
   * Interface to handle a response received by the server.
   */
  public interface TestResponseHandler {
    void handleResponse(HeronClient client,
                        StatusCode status,
                        Object ctx,
                        Message response) throws Exception;
  }

  /**
   * Generic SuccessResponseHandler that asserts that the response status code is OK and that the
   * message is of the expected type. After that assertion, delegates to delegate for additional
   * assertions if set.
   */
  public static final class SuccessResponseHandler
      implements TestResponseHandler {
    private final Class<? extends GeneratedMessageV3> expectedMessageClass;
    private final TestResponseHandler delegate;

    public SuccessResponseHandler(Class<? extends GeneratedMessageV3> expectedMessageClass) {
      this(expectedMessageClass, null);
    }

    public SuccessResponseHandler(Class<? extends GeneratedMessageV3> expectedMessageClass,
                                  TestResponseHandler delegate) {
      this.expectedMessageClass = expectedMessageClass;
      this.delegate = delegate;
    }

    @Override
    public void handleResponse(HeronClient client,
                               StatusCode status,
                               Object ctx, Message response) throws Exception {
      assertTrue(String.format(
          "Unexpected response message class received from the server. Expected: %s, Found: %s",
          expectedMessageClass.getName(), response.getClass().getName()),
          expectedMessageClass.isAssignableFrom(response.getClass()));
      assertEquals("Unexpected response code received from the server.", StatusCode.OK, status);
      if (delegate != null) {
        delegate.handleResponse(client, status, ctx, response);
      }
    }
  }
}
