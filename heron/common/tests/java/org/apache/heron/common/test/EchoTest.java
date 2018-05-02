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
import java.nio.channels.SocketChannel;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.common.network.HeronServer;
import org.apache.heron.common.network.REQID;
import org.apache.heron.common.network.StatusCode;
import org.apache.heron.common.testhelpers.HeronServerTester;
import org.apache.heron.proto.testing.Tests;

import static org.junit.Assert.assertEquals;

public class EchoTest {
  private static final int MAX_REQUESTS = 1000;

  private EchoServer server;
  private EchoClient client;
  private HeronServerTester heronServerTester;
  private CountDownLatch serverRequestsReceivedLatch;

  @Before
  public void before() throws IOException {
    int serverPort = SysUtils.getFreePort();
    serverRequestsReceivedLatch = new CountDownLatch(MAX_REQUESTS);
    server = new EchoServer(new NIOLooper(), serverPort, MAX_REQUESTS, serverRequestsReceivedLatch);
    client = new EchoClient(new NIOLooper(), serverPort, MAX_REQUESTS);
    heronServerTester = new HeronServerTester(server, client);
    heronServerTester.start();
  }

  @After
  public void after() {
    heronServerTester.stop();
  }

  @Test
  public void testStart() {
    HeronServerTester.await(serverRequestsReceivedLatch);
    assertEquals(MAX_REQUESTS, server.getRequestsCount());
    assertEquals(MAX_REQUESTS, client.getRequestsCount());
  }

  private static class EchoServer extends HeronServer {
    private static final Logger LOG = Logger.getLogger(EchoServer.class.getName());
    private int nRequests;
    private final int maxRequests;
    private final CountDownLatch requestsReceivedLatch;

    EchoServer(NIOLooper looper, int port, int maxRequests, CountDownLatch requestsReceivedLatch) {
      super(looper, HeronServerTester.SERVER_HOST, port, HeronServerTester.TEST_SOCKET_OPTIONS);
      this.nRequests = 0;
      this.maxRequests = maxRequests;
      this.requestsReceivedLatch = requestsReceivedLatch;
      registerOnRequest(Tests.EchoServerRequest.newBuilder());
    }

    private int getRequestsCount() {
      return nRequests;
    }

    @Override
    public void onConnect(SocketChannel channel) {
      LOG.info("A new client connected with us");
    }

    @Override
    public void onClose(SocketChannel channel) {
      LOG.info("A client closed connection");
    }

    @Override
    public void onRequest(REQID rid, SocketChannel channel, Message request) {
      if (request instanceof Tests.EchoServerRequest) {
        Tests.EchoServerResponse.Builder response = Tests.EchoServerResponse.newBuilder();
        Tests.EchoServerRequest req = (Tests.EchoServerRequest) request;
        response.setEchoResponse(req.getEchoRequest());
        sendResponse(rid, channel, response.build());
        nRequests++;
        if (nRequests % 10 == 0) {
          LOG.info("Processed " + nRequests + " requests");
        }

        requestsReceivedLatch.countDown();
        if (nRequests >= maxRequests) {
          // We wait for 1 second to let client to receive the request and then exit
          registerTimerEvent(Duration.ofSeconds(1),
              new Runnable() {
                @Override
                public void run() {
                  EchoServer.this.stop();
                  getNIOLooper().exitLoop();
                }
              });
        }
      } else {
        throw new RuntimeException("Unknown type of request received: " + request);
      }
    }

    @Override
    public void onMessage(SocketChannel channel, Message request) {
      throw new RuntimeException("Expected message from client: " + request);
    }
  }

  private static class EchoClient extends HeronServerTester.AbstractTestClient {
    private static final Logger LOG = Logger.getLogger(EchoClient.class.getName());
    private int nRequests;
    private int maxRequests;

    EchoClient(NIOLooper looper, int port, int maxRequests) {
      super(looper, HeronServerTester.SERVER_HOST, port, HeronServerTester.TEST_SOCKET_OPTIONS);
      this.nRequests = 0;
      this.maxRequests = maxRequests;
    }

    private int getRequestsCount() {
      return nRequests;
    }

    @Override
    public void onConnect(StatusCode status) {
      if (status != StatusCode.OK) {
        Assert.fail("Connection with server failed");
      } else {
        LOG.info("Connected with server");
        sendRequest();
      }
    }

    private void sendRequest() {
      if (nRequests >= maxRequests) {
        this.stop();
        getNIOLooper().exitLoop();
        return;
      }
      Tests.EchoServerRequest.Builder r = Tests.EchoServerRequest.newBuilder();
      r.setEchoRequest("Dummy");
      sendRequest(r.build(), Tests.EchoServerResponse.newBuilder());
      nRequests++;
    }

    @Override
    public void onResponse(StatusCode status, Object ctx, Message response) {
      if (response instanceof Tests.EchoServerResponse) {
        Tests.EchoServerResponse r = (Tests.EchoServerResponse) response;
        assertEquals(r.getEchoResponse(), "Dummy");
        sendRequest();
      } else {
        Assert.fail("Unknown type of response received");
      }
    }
  }
}
