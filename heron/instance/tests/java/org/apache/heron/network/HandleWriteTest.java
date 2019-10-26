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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.junit.Assert;
import org.junit.Test;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.network.IncomingPacket;
import org.apache.heron.common.network.OutgoingPacket;
import org.apache.heron.common.network.REQID;
import org.apache.heron.common.testhelpers.HeronServerTester;
import org.apache.heron.proto.stmgr.StreamManager;
import org.apache.heron.proto.system.Common;
import org.apache.heron.resource.UnitTestHelper;

/**
 * To test whether Instance's handleWrite() from the stream manager.
 * 1. The instance will connect to stream manager successfully
 * 2. We will construct a bunch of Mock Message and offer them to outStreamQueue.
 * 3. The instance should get items from outStreamQueue and send them to Stream Manager.
 * 4. Check whether items received in Stream Manager match the Mock Message we constructed in Instance.
 */
public class HandleWriteTest extends AbstractNetworkTest {

  /**
   * Test write into network
   */
  @Test
  public void testHandleWrite() throws IOException {
    ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
    serverSocketChannel.socket().bind(new InetSocketAddress(HOST, getServerPort()));

    SocketChannel socketChannel = null;
    try {
      StreamManagerClient streamManagerClient = runStreamManagerClient();

      socketChannel = acceptSocketChannel(serverSocketChannel);

      // Receive request
      REQID rid = readIncomingPacket(socketChannel).unpackREQID();

      OutgoingPacket outgoingPacket
          = new OutgoingPacket(rid, UnitTestHelper.getRegisterInstanceResponse());
      outgoingPacket.writeToChannel(socketChannel);

      HeronServerTester.await(getInControlQueueOfferLatch());

      for (int i = 0; i < 10; i++) {
        // We randomly choose some messages writing to stream mgr
        streamManagerClient.sendMessage(UnitTestHelper.getRegisterInstanceResponse());
      }

      for (int i = 0; i < 10; i++) {
        IncomingPacket incomingPacket = readIncomingPacket(socketChannel);
        incomingPacket.unpackREQID();

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
        Assert.assertEquals(TopologyAPI.TopologyState.RUNNING,
            response.getPplan().getTopology().getState());
      }

      getNIOLooper().exitLoop();

    } catch (ClosedChannelException ignored) {
    } finally {
      close(socketChannel);
    }
  }
}
