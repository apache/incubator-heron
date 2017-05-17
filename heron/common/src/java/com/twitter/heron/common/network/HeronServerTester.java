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
package com.twitter.heron.common.network;

import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.google.protobuf.GeneratedMessage;
import com.google.protobuf.Message;

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.common.basics.Communicator;
import com.twitter.heron.common.basics.NIOLooper;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Class to help simplify testing HeronServer implementations by consolidating common test
 * functionality. A new instance of this class should be used for each test. After initializing the
 * class, tests should call start(), which does the following:
 * <ol>
 * <li>starts the server and waits for it to come up</li>
 * <li>starts the client</li>
 * <li>calls sendMessage on the client using the TestRequestHandler</li>
 * <li>once the number of expected responses are recieved (via
 * TestResponseHandler.numExpectedResponses()) the TestResponseHandler.handleResponse method is
 * invoked</li>
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
      ByteAmount.fromMegabytes(5));
  private static final Duration SERVER_START_WAIT_TIME = Duration.ofSeconds(2);
  public static final Duration RESPONSE_RECEIVED_WAIT_TIME = Duration.ofSeconds(10);

  private final HeronServer server;
  private final TestRequestHandler requestHandler;
  private final TestResponseHandler responseHandler;
  private final ExecutorService threadsPool;

  private CountDownLatch serverStartedSignal;
  private CountDownLatch responseReceivedSignal;
  private HeronClient client;

  public HeronServerTester(HeronServer server,
                           TestRequestHandler requestHandler,
                           TestResponseHandler responseHandler) throws IOException {
    this.server = server;
    this.threadsPool = Executors.newFixedThreadPool(2);
    this.requestHandler = requestHandler;
    this.responseHandler = responseHandler;
    this.serverStartedSignal = new CountDownLatch(1);
    this.responseReceivedSignal = new CountDownLatch(1);
  }

  public void start() throws InterruptedException {
    // First run Server
    runServer();

    // Then run Client
    runClient();

    // Wait to make sure message was sent and response was received
    responseReceivedSignal.await(RESPONSE_RECEIVED_WAIT_TIME.toMillis(), TimeUnit.MILLISECONDS);
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
        try {
          NIOLooper looper = new NIOLooper();
          client = new TestClient(looper,
              server.getEndpoint().getHostName(), server.getEndpoint().getPort(),
              responseReceivedSignal, requestHandler, responseHandler);
          serverStartedSignal.await(SERVER_START_WAIT_TIME.toMillis(), TimeUnit.MILLISECONDS);
          client.start();
          looper.loop();
        } catch (IOException e) {
          throw new RuntimeException("Error instantiating client", e);
        } catch (InterruptedException e) {
          throw new RuntimeException("Timeout waiting for server to start", e);
        } finally {
          client.stop();
        }
      }
    };
    threadsPool.execute(runClient);
  }

  private static class TestClient extends HeronClient {
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
        fail("Connection with server failed");
      } else {
        LOG.info("Connected with Metrics Manager Server");
        sendRequest(requestHandler.getRequestMessage(), requestHandler.getResponseBuilder());
      }
    }

    @Override
    public void onError() {
      fail("Error in client while talking to server");
    }

    @Override
    public void onClose() {
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

    @Override
    public void onIncomingMessage(Message request) {
      fail("Expected message from client");
    }
  }

  public interface TestRequestHandler {
    Message getRequestMessage();
    Message.Builder getResponseBuilder();
  }

  public interface TestResponseHandler {
    void handleResponse(HeronClient client,
                        StatusCode status,
                        Object ctx,
                        Message response) throws Exception;
  }

  public static class TestCommunicator<E> extends Communicator<E> {
    private final CountDownLatch offersReceivedLatch;

    public TestCommunicator(int numExpectedOffers) {
      super();
      this.offersReceivedLatch = new CountDownLatch(numExpectedOffers);
    }

    public void awaitOffers(Duration timeout) throws InterruptedException {
      offersReceivedLatch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public boolean offer(E e) {
      boolean response = super.offer(e);
      offersReceivedLatch.countDown();
      return response;
    }
  }

  /**
   * Generic SuccessResponseHandler that asserts that the response status code is OK and that the
   * message is of the expected type. After that assertion, delegates to delegate for additional
   * assertions if set.
   */
  public static final class SuccessResponseHandler
      implements TestResponseHandler {
    private final Class<? extends GeneratedMessage> expectedMessageClass;
    private final TestResponseHandler delegate;

    public SuccessResponseHandler(Class<? extends GeneratedMessage> expectedMessageClass) {
      this(expectedMessageClass, null);
    }

    public SuccessResponseHandler(Class<? extends GeneratedMessage> expectedMessageClass,
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
