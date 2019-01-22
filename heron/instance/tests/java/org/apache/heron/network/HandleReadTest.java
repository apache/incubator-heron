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

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import org.junit.Assert;
import org.junit.Test;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.network.OutgoingPacket;
import org.apache.heron.common.network.REQID;
import org.apache.heron.common.testhelpers.HeronServerTester;
import org.apache.heron.proto.system.HeronTuples;
import org.apache.heron.resource.UnitTestHelper;

/**
 * To test whether Instance's handleRead() from the stream manager.
 * 1. The instance will connect to stream manager successfully
 * 2. We will construct a bunch of Mock Message and send them to Instance from Stream manager
 * 3. We will check the inStreamQueue for Instance, whether it contains the Mock Message we send from
 * Stream manager.
 */
public class HandleReadTest extends AbstractNetworkTest {
  private static final int SRC_TASK_ID = 1;

  /**
   * Test reading from network
   */
  @Test
  public void testHandleRead() throws IOException {
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

      outgoingPacket = new OutgoingPacket(REQID.zeroREQID, constructMockMessage());
      outgoingPacket.writeToChannel(socketChannel);

      HeronServerTester.await(getInStreamQueueOfferLatch());

      getNIOLooper().exitLoop();

      Assert.assertEquals(1, getInStreamQueue().size());

      Message msg = getInStreamQueue().poll();
      Assert.assertTrue(msg instanceof HeronTuples.HeronTupleSet);

      HeronTuples.HeronTupleSet heronTupleSet = (HeronTuples.HeronTupleSet) msg;

      Assert.assertTrue(heronTupleSet.hasData());
      Assert.assertFalse(heronTupleSet.hasControl());

      HeronTuples.HeronDataTupleSet heronDataTupleSet = heronTupleSet.getData();

      Assert.assertEquals("test-spout", heronDataTupleSet.getStream().getComponentName());
      Assert.assertEquals("default", heronDataTupleSet.getStream().getId());

      StringBuilder response = new StringBuilder();
      for (HeronTuples.HeronDataTuple heronDataTuple : heronDataTupleSet.getTuplesList()) {
        response.append(heronDataTuple.getValues(0).toStringUtf8());
        Assert.assertEquals(1, heronDataTuple.getRootsCount());
      }

      Assert.assertEquals("ABABABABAB", response.toString());
    } catch (ClosedChannelException ignored) {
    } finally {
      close(socketChannel);
    }
  }

  private Message constructMockMessage() {
    HeronTuples.HeronTupleSet2.Builder heronTupleSet = HeronTuples.HeronTupleSet2.newBuilder();
    heronTupleSet.setSrcTaskId(SRC_TASK_ID);
    HeronTuples.HeronDataTupleSet2.Builder dataTupleSet =
                                  HeronTuples.HeronDataTupleSet2.newBuilder();
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

      byte[] bytes = dataTuple.build().toByteArray();
      dataTupleSet.addTuples(ByteString.copyFrom(bytes));
    }

    heronTupleSet.setData(dataTupleSet);

    return heronTupleSet.build();
  }
}
