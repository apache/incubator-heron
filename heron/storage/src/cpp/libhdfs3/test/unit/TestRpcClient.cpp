/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "Exception.h"
#include "MockBufferedSocketReader.h"
#include "MockRpcChannel.h"
#include "MockSocket.h"
#include "rpc/RpcClient.h"
#include "TestRpcChannelStub.h"
#include "UnitTestUtils.h"
#include "XmlConfig.h"

using namespace Hdfs;
using namespace Hdfs::Mock;
using namespace Hdfs::Internal;
using namespace testing;

class MockRpcChannelStub: public TestRpcChannelStub {
public:
    MOCK_METHOD2(getChannel, RpcChannel * (
                     RpcChannelKey key, RpcClient & c));
};

static RpcChannelKey BuildKey() {
    RpcAuth auth;
    RpcProtocolInfo protocol(0, "test", "kind");
    RpcServerInfo server("unknown token service", "unknown server", "unknown port");
    Config conf;
    SessionConfig session(conf);
    RpcConfig rpcConf(session);
    return RpcChannelKey(auth, protocol, server, rpcConf);
}

static RpcConfig & GetConfig(RpcChannelKey & key) {
    return const_cast<RpcConfig &>(key.getConf());
}

TEST(TestRpcClient, TestInvoke_CreateChannelFailure) {
    RpcAuth auth;
    RpcProtocolInfo protocol(0, "test", "kind");
    RpcServerInfo server("unknown token service", "unknown server", "unknown port");
    Config conf;
    SessionConfig session(conf);
    RpcConfig rpcConf(session);
    MockRpcChannelStub stub;
    RpcClientImpl client;
    client.stub = &stub;
    EXPECT_CALL(stub, getChannel(_, _)).Times(1).WillOnce(
        InvokeWithoutArgs(
            bind(&InvokeThrowAndReturn<HdfsIOException, RpcChannel *>,
                 "create channel failed.", static_cast<bool *>(NULL),
                 static_cast<RpcChannel *>(NULL))));
    EXPECT_THROW(client.getChannel(auth, protocol, server, rpcConf),
                 HdfsRpcException);
}

TEST(TestRpcClient, TestCleanupIdleChannel) {
    RpcAuth auth;
    RpcProtocolInfo protocol(0, "test", "kind");
    RpcServerInfo server("unknown token service", "unknown server", "unknown port");
    Config conf;
    SessionConfig session(conf);
    RpcConfig rpcConf(session);
    MockRpcChannelStub stub;
    RpcClientImpl client;
    client.stub = &stub;
    RpcChannelKey key = BuildKey();
    GetConfig(key).setMaxIdleTime(10000);
    GetConfig(key).setPingTimeout(900);
    MockSocket * sock = new MockSocket();
    MockBufferedSocketReader * in = new MockBufferedSocketReader();
    RpcChannelImpl * channel = new RpcChannelImpl(key, sock, in, client);
    EXPECT_CALL(stub, getChannel(_, _)).Times(1).WillOnce(Return(channel));
    channel->lastActivity = channel->lastIdle = steady_clock::now();
    channel->available = true;
    EXPECT_CALL(*sock, close()).Times(AtLeast(1));
    EXPECT_CALL(*sock, writeFully(_, _, _)).Times(AtLeast(1));
    sleep_for(seconds(1));
    EXPECT_NO_THROW(client.getChannel(auth, protocol, server, rpcConf));
    EXPECT_TRUE(channel->refs == 1);
    channel->close(false);
    sleep_for(seconds(3));
}
