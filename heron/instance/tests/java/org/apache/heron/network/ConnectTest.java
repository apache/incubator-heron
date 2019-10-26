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
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;

import org.junit.Assert;
import org.junit.Test;

import org.apache.heron.common.network.OutgoingPacket;
import org.apache.heron.common.network.REQID;
import org.apache.heron.common.testhelpers.HeronServerTester;
import org.apache.heron.common.utils.misc.PhysicalPlanHelper;
import org.apache.heron.instance.InstanceControlMsg;
import org.apache.heron.resource.UnitTestHelper;

import static org.junit.Assert.assertNotNull;

/**
 * To test whether Instance could connect to stream manager successfully.
 * It will have a mock stream manager, which will:
 * 1. Open a socket, and waiting for the RegisterInstanceRequest constructed by us
 * 2. Once receiving a RegisterInstanceRequest, check whether the RegisterInstanceRequest's info matches
 * the one we constructed.
 * 3. Send back a mock RegisterInstanceResponse with Physical Plan.
 * 4. Check whether the Instance adds the Physical Plan to the singletonRegistry.
 */

public class ConnectTest extends AbstractNetworkTest {

  /**
   * Test connection
   */
  @Test
  public void testStart() throws IOException {
    ServerSocketChannel serverSocketChannel = ServerSocketChannel.open();
    serverSocketChannel.socket().bind(new InetSocketAddress(HOST, getServerPort()));

    SocketChannel socketChannel = null;
    try {
      runStreamManagerClient();

      socketChannel = acceptSocketChannel(serverSocketChannel);

      // Receive request
      REQID rid = readIncomingPacket(socketChannel).unpackREQID();

      OutgoingPacket outgoingPacket
          = new OutgoingPacket(rid, UnitTestHelper.getRegisterInstanceResponse());
      outgoingPacket.writeToChannel(socketChannel);

      HeronServerTester.await(getInControlQueueOfferLatch());

      InstanceControlMsg instanceControlMsg = getInControlQueue().poll();
      assertNotNull(instanceControlMsg);

      getNIOLooper().exitLoop();
      getThreadPool().shutdownNow();

      PhysicalPlanHelper physicalPlanHelper = instanceControlMsg.getNewPhysicalPlanHelper();

      Assert.assertEquals("test-bolt", physicalPlanHelper.getMyComponent());
      Assert.assertEquals(InetAddress.getLocalHost().getHostName(),
          physicalPlanHelper.getMyHostname());
      Assert.assertEquals(0, physicalPlanHelper.getMyInstanceIndex());
      Assert.assertEquals(1, physicalPlanHelper.getMyTaskId());

    } catch (ClosedChannelException ignored) {
    } finally {
      close(socketChannel);
    }
  }
}
