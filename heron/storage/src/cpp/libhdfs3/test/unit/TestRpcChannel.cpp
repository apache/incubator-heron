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

#include "Atomic.h"
#include "ClientNamenodeProtocol.pb.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "IpcConnectionContext.pb.h"
#include "Logger.h"
#include "MockBufferedSocketReader.h"
#include "MockRpcClient.h"
#include "MockRpcRemoteCall.h"
#include "MockSocket.h"
#include "rpc/RpcChannel.h"
#include "RpcHeader.pb.h"
#include "TestUtil.h"
#include "Thread.h"
#include "UnitTestUtils.h"

#include <google/protobuf/io/coded_stream.h>

using namespace Hdfs;
using namespace Hdfs::Mock;
using namespace Hdfs::Internal;
using namespace testing;
using namespace ::google::protobuf;
using namespace ::google::protobuf::io;

RpcChannelKey BuildKey() {
    RpcAuth auth;
    RpcProtocolInfo protocol(0, "test", "kind");
    RpcServerInfo server("unknown token server", "unknown server", "unknown port");
    Config conf;
    SessionConfig session(conf);
    RpcConfig rpcConf(session);
    return RpcChannelKey(auth, protocol, server, rpcConf);
}

static RpcConfig & GetConfig(RpcChannelKey & key) {
    return const_cast<RpcConfig &>(key.getConf());
}

static uint32_t GetCallId(uint32_t start) {
    static Hdfs::Internal::atomic<uint32_t> id(start);
    return id++;
}

static void BuildResponse(uint32_t id,
                          RpcResponseHeaderProto::RpcStatusProto status, const char * errClass,
                          const char * errMsg, ::google::protobuf::Message * resp, std::vector<char> & output) {
    WriteBuffer buffer;
    RpcResponseHeaderProto respHeader;
    respHeader.set_callid(id);
    respHeader.set_status(status);

    if (errClass != NULL) {
        respHeader.set_exceptionclassname(errClass);
    }

    if (errMsg != NULL) {
        respHeader.set_errormsg(errMsg);
    }

    int size = respHeader.ByteSize();
    int totalSize = size + CodedOutputStream::VarintSize32(size);

    if (resp != NULL) {
        size = resp->ByteSize();
        totalSize += size + CodedOutputStream::VarintSize32(size);
    }

    buffer.writeBigEndian(totalSize);
    size = respHeader.ByteSize();
    buffer.writeVarint32(size);
    respHeader.SerializeToArray(buffer.alloc(size), size);

    if (resp != NULL) {
        size = resp->ByteSize();
        buffer.writeVarint32(size);
        resp->SerializeToArray(buffer.alloc(size), size);
    }

    output.resize(buffer.getDataSize(0));
    memcpy(&output[0], buffer.getBuffer(0), buffer.getDataSize(0));
}

TEST(TestRpcChannel, TestConnectFailure) {
    RpcChannelKey key = BuildKey();
    MockRpcClient client;
    EXPECT_CALL(client, getClientId()).Times(AnyNumber()).WillRepeatedly(Return(""));
    MockSocket * sock = new MockSocket();
    MockBufferedSocketReader * in = new MockBufferedSocketReader();
    GetConfig(key).setMaxRetryOnConnect(2);
    EXPECT_CALL(*sock, connect(An<const char *>(), An<const char *>(), _)).Times(2).WillOnce(
        InvokeWithoutArgs(bind(&InvokeThrow<Hdfs::HdfsNetworkException>,
                               "test expected connect failed",
                               static_cast<bool *>(NULL)))).WillOnce(
                                   InvokeWithoutArgs(
                                       bind(&InvokeThrow<Hdfs::HdfsTimeoutException>,
                                            "test expected connect timeout",
                                            static_cast<bool *>(NULL))));
    EXPECT_CALL(*sock, close()).Times(2);
    RpcChannelImpl channel(key, sock, in, client);
    EXPECT_THROW(DebugException(channel.connect()), Hdfs::HdfsTimeoutException);
}

TEST(TestRpcChannel, TestConnectSuccess) {
    RpcChannelKey key = BuildKey();
    MockRpcClient client;
    EXPECT_CALL(client, getClientId()).Times(AnyNumber()).WillRepeatedly(Return(""));
    MockSocket * sock = new MockSocket();
    MockBufferedSocketReader * in = new MockBufferedSocketReader();
    GetConfig(key).setMaxRetryOnConnect(1);
    EXPECT_CALL(*sock, connect(An<const char *>(), An<const char *>(), _)).Times(1);
    EXPECT_CALL(*sock, close()).Times(1);
    EXPECT_CALL(*sock, setNoDelay(_)).Times(1);
    EXPECT_CALL(*sock, writeFully(_, _, _)).Times(2);
    RpcChannelImpl channel(key, sock, in, client);
    EXPECT_NO_THROW(DebugException(channel.connect()));
    EXPECT_TRUE(channel.available);
}

TEST(TestRpcChannel, TestCancelPendingCall) {
    RpcChannelKey key = BuildKey();
    MockRpcClient client;
    EXPECT_CALL(client, getClientId()).Times(AnyNumber()).WillRepeatedly(Return(""));
    MockSocket * sock = new MockSocket();
    MockBufferedSocketReader * in = new MockBufferedSocketReader();
    RpcRemoteCallPtr call = RpcRemoteCallPtr(
                                new MockRpcRemoteCall(RpcCall(false, "", NULL, NULL), 0, ""));
    EXPECT_CALL(*sock, close()).Times(1);
    EXPECT_CALL(*static_cast<MockRpcRemoteCall *>(call.get()), cancel(_)).Times(1);
    RpcChannelImpl channel(key, sock, in, client);
    channel.available = true;
    channel.pendingCalls[call->getIdentity()] = call;

    try {
        THROW(HdfsNetworkException, "expected exception");
    } catch (...) {
        Hdfs::Internal::lock_guard<Hdfs::Internal::mutex> lock(channel.writeMut);
        EXPECT_NO_THROW(DebugException(channel.cleanupPendingCalls(Hdfs::current_exception())));
    }
}

TEST(TestRpcChannel, TestCheckResponse_InvalidResponse) {
    RpcChannelKey key = BuildKey();
    MockRpcClient client;
    EXPECT_CALL(client, getClientId()).Times(AnyNumber()).WillRepeatedly(Return(""));
    MockSocket * sock = new MockSocket();
    MockBufferedSocketReader * in = new MockBufferedSocketReader();
    EXPECT_CALL(*in, readBigEndianInt32(_)).Times(1).WillOnce(Return(100));
    EXPECT_CALL(*in, readVarint32(_)).Times(1).WillOnce(Return(0));
    EXPECT_CALL(*in, readFully(_, _, _)).Times(1);
    EXPECT_CALL(client, isRunning()).Times(AnyNumber()).WillRepeatedly(
        Return(true));
    EXPECT_CALL(*sock, close()).Times(1);
    RpcChannelImpl channel(key, sock, in, client);
    channel.available = true;
    EXPECT_THROW(channel.readOneResponse(false), HdfsRpcException);
}

TEST(TestRpcChannel, TestCheckResponse_InvalidCallId) {
    std::vector<char> resp;
    BuildResponse(0, RpcResponseHeaderProto_RpcStatusProto_SUCCESS, NULL, NULL, NULL, resp);
    RpcChannelKey key = BuildKey();
    MockRpcClient client;
    EXPECT_CALL(client, getClientId()).Times(AnyNumber()).WillRepeatedly(Return(""));
    MockSocket * sock = new MockSocket();
    BufferedSocketReaderImpl * in = new BufferedSocketReaderImpl(*sock, resp);
    EXPECT_CALL(*sock, close()).Times(1);
    EXPECT_CALL(client, isRunning()).Times(AnyNumber()).WillRepeatedly(
        Return(true));
    RpcChannelImpl channel(key, sock, in, client);
    channel.available = true;
    EXPECT_THROW(channel.readOneResponse(false), HdfsRpcException);
}

TEST(TestRpcChannel, TestCheckResponse_SendPing) {
    RpcChannelKey key = BuildKey();
    MockRpcClient client;
    EXPECT_CALL(client, getClientId()).Times(AnyNumber()).WillRepeatedly(Return(""));
    MockSocket * sock = new MockSocket();
    MockBufferedSocketReader * in = new MockBufferedSocketReader();
    GetConfig(key).setMaxRetryOnConnect(1);
    GetConfig(key).setMaxIdleTime(1000);
    GetConfig(key).setPingTimeout(400);
    EXPECT_CALL(*sock, close()).Times(1);
    EXPECT_CALL(*sock, writeFully(_, _, _)).Times(1);
    EXPECT_CALL(*in, poll(_)).Times(2).WillOnce(
        InvokeWithoutArgs(bind(&InvokeWaitAndReturn<bool>, 500, false, 0))).WillOnce(
            InvokeWithoutArgs(
                bind(&InvokeThrowAndReturn<HdfsNetworkException, bool>,
                     "expected exception", static_cast<bool *>(NULL),
                     false)));
    EXPECT_CALL(client, isRunning()).Times(AnyNumber()).WillRepeatedly(
        Return(true));
    RpcChannelImpl channel(key, sock, in, client);
    channel.lastActivity = steady_clock::now();
    channel.available = true;
    EXPECT_THROW(channel.checkOneResponse(), HdfsNetworkException);
}

TEST(TestRpcChannel, TestCheckResponse_ErrorResponse) {
    std::vector<char> resp;
    const char * errClass = "test error class";
    const char * errMsg = "test error message";
    BuildResponse(0, RpcResponseHeaderProto_RpcStatusProto_ERROR, errClass, errMsg, NULL, resp);
    RpcChannelKey key = BuildKey();
    MockRpcClient client;
    EXPECT_CALL(client, getClientId()).Times(AnyNumber()).WillRepeatedly(Return(""));
    MockSocket * sock = new MockSocket();
    BufferedSocketReaderImpl * in = new BufferedSocketReaderImpl(*sock, resp);
    RpcRemoteCallPtr call = RpcRemoteCallPtr(
                                new MockRpcRemoteCall(RpcCall(false, "", NULL, NULL), 0, ""));
    EXPECT_CALL(client, isRunning()).Times(AnyNumber()).WillRepeatedly(
        Return(true));
    EXPECT_CALL(*sock, close()).Times(1);
    EXPECT_CALL(*static_cast<MockRpcRemoteCall *>(call.get()), cancel(_)).Times(1);
    RpcChannelImpl channel(key, sock, in, client);
    channel.available = true;
    channel.pendingCalls[call->getIdentity()] = call;
    EXPECT_NO_THROW(DebugException(channel.readOneResponse(false)));
}

TEST(TestRpcChannel, TestCheckResponse_FatalResponse1) {
    std::vector<char> resp;
    const char * errMsg = "test fatal message";
    BuildResponse(0, RpcResponseHeaderProto_RpcStatusProto_FATAL, NULL, errMsg, NULL, resp);
    RpcChannelKey key = BuildKey();
    MockRpcClient client;
    EXPECT_CALL(client, getClientId()).Times(AnyNumber()).WillRepeatedly(Return(""));
    MockSocket * sock = new MockSocket();
    BufferedSocketReaderImpl * in = new BufferedSocketReaderImpl(*sock, resp);
    EXPECT_CALL(client, isRunning()).Times(AnyNumber()).WillRepeatedly(
        Return(true));
    EXPECT_CALL(*sock, close()).Times(1);
    RpcChannelImpl channel(key, sock, in, client);
    channel.available = true;
    EXPECT_THROW(DebugException(channel.readOneResponse(false)), HdfsRpcException);
}

TEST(TestRpcChannel, TestCheckResponse_FatalResponse2) {
    std::vector<char> resp;
    const char * errClass = "org.apache.hadoop.ipc.StandbyException";
    const char * errMsg = "test fatal message";
    BuildResponse(0, RpcResponseHeaderProto_RpcStatusProto_FATAL, errClass, errMsg, NULL, resp);
    RpcChannelKey key = BuildKey();
    MockRpcClient client;
    EXPECT_CALL(client, getClientId()).Times(AnyNumber()).WillRepeatedly(Return(""));
    MockSocket * sock = new MockSocket();
    BufferedSocketReaderImpl * in = new BufferedSocketReaderImpl(*sock, resp);
    EXPECT_CALL(client, isRunning()).Times(AnyNumber()).WillRepeatedly(
        Return(true));
    EXPECT_CALL(*sock, close()).Times(1);
    RpcChannelImpl channel(key, sock, in, client);
    channel.available = true;
    EXPECT_THROW(DebugException(channel.readOneResponse(false)), NameNodeStandbyException);
}

TEST(TestRpcChannel, TestInvoke_RetryIdempotent) {
    std::vector<char> respBody;
    MockRpcClient client;
    uint32_t callid = 2;
    EXPECT_CALL(client, getClientId()).Times(AnyNumber()).WillRepeatedly(Return(""));
    EXPECT_CALL(client, getCallId()).Times(AnyNumber()).WillRepeatedly(Return(callid));
    MkdirsRequestProto request;
    MkdirsResponseProto response, resp;
    request.set_src("src");
    request.set_createparent(true);
    request.mutable_masked()->set_perm(0600u);
    resp.set_result(true);
    BuildResponse(callid, RpcResponseHeaderProto_RpcStatusProto_SUCCESS, NULL, NULL, &resp, respBody);
    MockSocket * sock = new MockSocket();
    RpcChannelKey key = BuildKey();
    BufferedSocketReaderImpl * in = new BufferedSocketReaderImpl(*sock, respBody);
    EXPECT_CALL(client, isRunning()).Times(AnyNumber()).WillRepeatedly(
        Return(true));
    EXPECT_CALL(*sock, connect(An<const char *>(), An<const char *>(), _)).Times(1);
    EXPECT_CALL(*sock, setNoDelay(_)).Times(1);
    EXPECT_CALL(*sock, close()).Times(AtLeast(2));
    EXPECT_CALL(*sock, writeFully(_, _, _)).Times(4).WillOnce(
        InvokeWithoutArgs(
            bind(&InvokeThrow<HdfsNetworkConnectException>,
                 "expected exception", static_cast<bool *>(NULL)))).WillRepeatedly(
                     InvokeWithoutArgs(&InvokeDoNothing));
    RpcChannelImpl channel(key, sock, in, client);
    channel.available = true;
    channel.addRef();
    EXPECT_NO_THROW(DebugException(channel.invoke(RpcCall(true, "mkdirs", &request, &response))));
    channel.close(false);
}

static void InvokeConcurrently(RpcChannelImpl * channel) {
    MkdirsRequestProto request;
    MkdirsResponseProto response;
    request.set_src("src");
    request.set_createparent(true);
    request.mutable_masked()->set_perm(0600u);
    channel->addRef();
    EXPECT_THROW(DebugException(channel->invoke(RpcCall(true, "mkdirs", &request, &response))),
                 HdfsRpcException);
    channel->close(false);
}

TEST(TestRpcChannel, TestInvokeConcurrently_FatalError) {
    bool throwError = false;
    MockRpcClient client;
    EXPECT_CALL(client, getClientId()).Times(AnyNumber()).WillRepeatedly(Return(""));
    EXPECT_CALL(client, getCallId()).Times(AnyNumber()).WillRepeatedly(
        InvokeWithoutArgs(bind(GetCallId, 0)));
    RpcChannelKey key = BuildKey();
    MockSocket * sock = new MockSocket();
    MockBufferedSocketReader * in = new MockBufferedSocketReader();
    EXPECT_CALL(client, isRunning()).Times(AnyNumber()).WillRepeatedly(
        Return(true));
    EXPECT_CALL(*in, poll(_)).Times(AnyNumber()).WillRepeatedly(
        InvokeWithoutArgs(
            bind(&InvokeThrowAndReturn<HdfsNetworkException, bool>,
                 "expected exception", &throwError, false)));
    EXPECT_CALL(*sock, writeFully(_, _, _)).Times(AnyNumber()).WillRepeatedly(
        InvokeWithoutArgs(&InvokeDoNothing));
    EXPECT_CALL(*sock, close()).Times(AnyNumber());
    EXPECT_CALL(*sock, setNoDelay(_)).Times(AnyNumber());
    RpcChannelImpl channel(key, sock, in, client);
    EXPECT_CALL(*sock, connect(An<const char *>(), An<const char *>(), _)).Times(
        AnyNumber()).WillRepeatedly(Assign(&channel.available, true));
    std::vector<std::shared_ptr<thread> > threads;
    int numOfThread = 50;

    for (int i = 0; i < numOfThread; ++i) {
        threads.push_back(
            std::shared_ptr<thread>(
                new thread(InvokeConcurrently, &channel)));
    }

    sleep_for(seconds(5));
    throwError = true;

    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i]->join();
    }
}

TEST(TestRpcChannel, TestInvokeConcurrently_ClientExit) {
    bool clientIsRunning = true;
    MockRpcClient client;
    EXPECT_CALL(client, getClientId()).Times(AnyNumber()).WillRepeatedly(Return(""));
    RpcChannelKey key = BuildKey();
    MockSocket * sock = new MockSocket();
    MockBufferedSocketReader * in = new MockBufferedSocketReader();
    EXPECT_CALL(client, isRunning()).Times(AnyNumber()).WillRepeatedly(
        ReturnPointee(&clientIsRunning));
    EXPECT_CALL(*in, poll(_)).Times(AnyNumber()).WillRepeatedly(Return(false));
    EXPECT_CALL(*sock, writeFully(_, _, _)).Times(AnyNumber()).WillRepeatedly(
        InvokeWithoutArgs(&InvokeDoNothing));
    EXPECT_CALL(*sock, close()).Times(AnyNumber());
    RpcChannelImpl channel(key, sock, in, client);
    channel.available = true;
    std::vector<std::shared_ptr<thread> > threads;
    EXPECT_CALL(client, getCallId()).Times(AnyNumber()).WillRepeatedly(
        InvokeWithoutArgs(bind(GetCallId, 0)));
    int numOfThread = 50;

    for (int i = 0; i < numOfThread; ++i) {
        threads.push_back(
            std::shared_ptr<thread>(
                new thread(InvokeConcurrently, &channel)));
    }

    for (int i = 0 ; i < 5; ++i) {
        channel.checkIdle();
        sleep_for(seconds(1));
    }

    clientIsRunning = false;

    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i]->join();
    }
}


static void TestTimeout(RpcChannelImpl & channel, RpcCall & call) {
    try {
        channel.invoke(call);
    } catch (const HdfsRpcException & e) {
        Hdfs::rethrow_if_nested(e);
    }
}

TEST(TestRpcChannel, TestTimeout) {
    MockRpcClient client;
    EXPECT_CALL(client, getClientId()).Times(AnyNumber()).WillRepeatedly(Return(""));
    EXPECT_CALL(client, getCallId()).Times(AnyNumber()).WillRepeatedly(InvokeWithoutArgs(bind(GetCallId, 0)));
    RpcChannelKey key = BuildKey();
    GetConfig(key).setRpcTimeout(1000);
    MockSocket * sock = new MockSocket();
    MockBufferedSocketReader * in = new MockBufferedSocketReader();
    EXPECT_CALL(client, isRunning()).Times(AnyNumber()).WillRepeatedly(Return(true));
    EXPECT_CALL(*sock, writeFully(_, _, _)).Times(AnyNumber()).WillRepeatedly(Return());
    EXPECT_CALL(*sock, close()).Times(AnyNumber()).WillRepeatedly(Return());
    EXPECT_CALL(*in, poll(_)).Times(AnyNumber()).WillRepeatedly(Return(false));
    RpcChannelImpl channel(key, sock, in, client);
    channel.available = true;
    ++channel.refs;
    MkdirsRequestProto request;
    MkdirsResponseProto response;
    request.set_src("src");
    request.set_createparent(true);
    request.mutable_masked()->set_perm(0600u);
    RpcCall call(false, "mkdirs", &request, &response);
    EXPECT_THROW(TestTimeout(channel, call), HdfsTimeoutException);
    --channel.refs;
}


