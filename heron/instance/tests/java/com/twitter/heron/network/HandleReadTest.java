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
import java.net.InetSocketAddress;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.time.Duration;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.network.IncomingPacket;
import com.twitter.heron.common.network.OutgoingPacket;
import com.twitter.heron.common.network.REQID;
import com.twitter.heron.instance.InstanceControlMsg;
import com.twitter.heron.proto.stmgr.StreamManager;
import com.twitter.heron.proto.system.HeronTuples;
import com.twitter.heron.resource.Constants;
import com.twitter.heron.resource.UnitTestHelper;

/**
 * To test whether Instance's handleRead() from the stream manager.
 * 1. The instance will connect to stream manager successfully
 * 2. We will construct a bunch of Mock Message and send them to Instance from Stream manager
 * 3. We will check the inStreamQueue for Instance, whether it contains the Mock Message we send from
 * Stream manager.
 */
public class HandleReadTest extends AbstractNetworkTest {

  /**
   * Test reading from network
   */
  @Test
  public void testHandleRead() throws IOException {
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
      String typeName = incomingPacket.unpackString();
      REQID rid = incomingPacket.unpackREQID();

      OutgoingPacket outgoingPacket
          = new OutgoingPacket(rid, UnitTestHelper.getRegisterInstanceResponse());
      outgoingPacket.writeToChannel(socketChannel);

      for (int i = 0; i < Constants.RETRY_TIMES; i++) {
        InstanceControlMsg instanceControlMsg = getInControlQueue().poll();
        if (instanceControlMsg != null) {
          break;
        } else {
          SysUtils.sleep(Constants.RETRY_INTERVAL);
        }
      }

      outgoingPacket = new OutgoingPacket(REQID.zeroREQID, constructMockMessage());
      outgoingPacket.writeToChannel(socketChannel);

      for (int i = 0; i < Constants.RETRY_TIMES; i++) {
        if (!getInStreamQueue().isEmpty()) {
          break;
        }
        SysUtils.sleep(Constants.RETRY_INTERVAL);
      }
      getNIOLooper().exitLoop();

      Assert.assertEquals(1, getInStreamQueue().size());
      HeronTuples.HeronTupleSet msg = getInStreamQueue().poll();

      HeronTuples.HeronTupleSet heronTupleSet = msg;

      Assert.assertTrue(heronTupleSet.hasData());
      Assert.assertFalse(heronTupleSet.hasControl());

      HeronTuples.HeronDataTupleSet heronDataTupleSet = heronTupleSet.getData();

      Assert.assertEquals("test-spout", heronDataTupleSet.getStream().getComponentName());
      Assert.assertEquals("default", heronDataTupleSet.getStream().getId());

      String res = "";
      for (HeronTuples.HeronDataTuple heronDataTuple : heronDataTupleSet.getTuplesList()) {
        res += heronDataTuple.getValues(0).toStringUtf8();
        Assert.assertEquals(1, heronDataTuple.getRootsCount());
      }

      Assert.assertEquals("ABABABABAB", res);
    } catch (ClosedChannelException ignored) {
    } finally {
      close(socketChannel);
    }
  }

  private Message constructMockMessage() {
    StreamManager.TupleMessage.Builder message = StreamManager.TupleMessage.newBuilder();
    HeronTuples.HeronTupleSet.Builder heronTupleSet = HeronTuples.HeronTupleSet.newBuilder();
    HeronTuples.HeronDataTupleSet.Builder dataTupleSet = HeronTuples.HeronDataTupleSet.newBuilder();
    TopologyAPI.StreamId.Builder streamId = TopologyAPI.StreamId.newBuilder();
    streamId.setComponentName("test-spout");
    streamId.setId("default");
    dataTupleSet.setStream(streamId);

    // We will add 10 tuples to the set
    for (int i = 0; i < 10; i++) {
      HeronTuples.HeronDataTuple.Builder dataTuple = HeronTuples.HeronDataTuple.newBuilder();
      dataTuple.setKey(19901017 + i);

      HeronTuples.RootId.Builder rootId = HeronTuples.RootId.newBuilder();
      rootId.setKey(19901017 + i);
      rootId.setTaskid(0);
      dataTuple.addRoots(rootId);

      String tupleData = ((i & 1) == 0) ? "A" : "B";
      dataTuple.addValues(ByteString.copyFrom(tupleData.getBytes()));

      dataTupleSet.addTuples(dataTuple);
    }

    heronTupleSet.setData(dataTupleSet);
    message.setSet(heronTupleSet);

    return message.build();
  }
}
