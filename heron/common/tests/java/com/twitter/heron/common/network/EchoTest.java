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

import java.io.IOException;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

import com.google.protobuf.Message;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.heron.common.basics.ByteAmount;
import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.proto.testing.Tests;

public class EchoTest {
  private static int serverPort;
  private ExecutorService threadsPool;
  private static final HeronSocketOptions TEST_SOCKET_OPTIONS = new HeronSocketOptions(
      ByteAmount.fromMegabytes(100), 100,
      ByteAmount.fromMegabytes(100),100,
      ByteAmount.fromMegabytes(5),
      ByteAmount.fromMegabytes(5));

  @BeforeClass
  public static void beforeClass() throws Exception {
  }

  @AfterClass
  public static void afterClass() throws Exception {
  }

  @Before
  public void before() throws Exception {
    threadsPool = Executors.newSingleThreadExecutor();

    // Get an available port
    serverPort = SysUtils.getFreePort();
  }

  @After
  public void after() throws Exception {
    threadsPool.shutdownNow();
    threadsPool = null;
  }

  @Test
  public void testStart() throws Exception {
    runServer();
    // We'll sleep to give the server a chance to bind and start listening
    Thread.sleep(1000);

    runClient();
  }

  private void runServer() {
    Runnable server = new Runnable() {
      @Override
      public void run() {
        NIOLooper looper;
        try {
          looper = new NIOLooper();
          EchoServer s = new EchoServer(looper, serverPort, 1000);
          s.start();
        } catch (IOException e) {
          throw new RuntimeException("Some error instantiating server");
        }

        looper.loop();
      }
    };
    threadsPool.execute(server);
  }

  private void runClient() {
    NIOLooper looper;
    try {
      looper = new NIOLooper();
      EchoClient c = new EchoClient(looper, serverPort, 1000);
      c.start();
    } catch (IOException e) {
      throw new RuntimeException("Some error instantiating client");
    }

    looper.loop();
  }

  private static class EchoServer extends HeronServer {
    private static final Logger LOG = Logger.getLogger(EchoServer.class.getName());
    private int nRequests;
    private int maxRequests;

    EchoServer(NIOLooper looper, int port, int maxRequests) {
      super(looper, "localhost", port, TEST_SOCKET_OPTIONS);
      nRequests = 0;
      this.maxRequests = maxRequests;
      registerOnRequest(Tests.EchoServerRequest.newBuilder());
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
        if (nRequests >= maxRequests) {
          // We wait for 1 second to let client to receive the request and then exit
          registerTimerEventInSeconds(1,
              new Runnable() {
                @Override
                public void run() {
                  EchoServer.this.stop();
                  getNIOLooper().exitLoop();
                  return;
                }
              });
        }
      } else {
        throw new RuntimeException("Unknown type of request received");
      }
    }

    @Override
    public void onMessage(SocketChannel channel, Message request) {
      throw new RuntimeException("Expected message from client");
    }
  }

  private static class EchoClient extends HeronClient {
    private static final Logger LOG = Logger.getLogger(EchoClient.class.getName());
    private HeronSocketOptions socketOptions;
    private int nRequests;
    private int maxRequests;

    EchoClient(NIOLooper looper, int port, int maxRequests) {
      super(looper, "localhost", port, TEST_SOCKET_OPTIONS);
      nRequests = 0;
      this.maxRequests = maxRequests;
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

    @Override
    public void onError() {
      Assert.fail("Error in client while talking to server");
    }

    @Override
    public void onClose() {

    }

    private void sendRequest() {
      if (nRequests > maxRequests) {
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
        Assert.assertEquals(r.getEchoResponse(), "Dummy");
        sendRequest();
      } else {
        Assert.fail("Unknown type of response received");
      }
    }

    @Override
    public void onIncomingMessage(Message request) {
      Assert.fail("Expected message from client");
    }
  }
}
