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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.network.IncomingPacket;
import com.twitter.heron.common.network.OutgoingPacket;
import com.twitter.heron.common.network.REQID;
import com.twitter.heron.common.utils.misc.PhysicalPlanHelper;
import com.twitter.heron.instance.InstanceControlMsg;
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

public class ConnectTest extends AbstractNetworkTest {

  /**
   * Test connection
   */
  @Test
  public void testStart() throws IOException {
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
      REQID rid = incomingPacket.unpackREQID();

      OutgoingPacket outgoingPacket
          = new OutgoingPacket(rid, UnitTestHelper.getRegisterInstanceResponse());
      outgoingPacket.writeToChannel(socketChannel);

      for (int i = 0; i < Constants.RETRY_TIMES; i++) {
        InstanceControlMsg instanceControlMsg = getInControlQueue().poll();
        if (instanceControlMsg != null) {
          getNIOLooper().exitLoop();
          getThreadPool().shutdownNow();

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
}
