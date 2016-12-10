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

#include <errno.h>
#include <netdb.h>
#include <sys/socket.h>

#include "Exception.h"
#include "ExceptionInternal.h"
#include "MockOperationCanceledCallback.h"
#include "MockSockCall.h"
#include "MockSystem.h"
#include "network/TcpSocket.h"
#include "Thread.h"
#include "UnitTestUtils.h"

using namespace Hdfs;
using namespace Hdfs::Mock;
using namespace Hdfs::Internal;
using namespace MockSystem;
using namespace testing;

class TestSocket: public ::testing::Test {
public:
    virtual void SetUp() {
        MockSockSysCallObj = &syscall;
    }

    virtual void TearDown() {
        MockSockSysCallObj = NULL;
        ChecnOperationCanceledCallback = function<bool(void)>();
    }

public:
    MockSockSysCall syscall;
};

static int32_t ReadAction(int sock, void * buffer, int32_t size, int flag,
                          const void * target, int32_t tsize) {
    assert(size >= tsize);
    memcpy(buffer, target, tsize);
    return tsize;
}

static int32_t WriteAction(int sock, const void * buffer, int32_t size,
                           int flag, void * target, int32_t tsize) {
    int32_t retval = size < tsize ? size : tsize;
    memcpy(target, buffer, tsize);
    return retval;
}

TEST_F(TestSocket, ConnectFailure_Socket) {
    struct addrinfo addr;
    TcpSocketImpl sock;
    EXPECT_CALL(syscall, socket(_, _, _)).Times(1).WillOnce(Return(-1));
    EXPECT_THROW(sock.connect(&addr, "host", "port", 1000),
                 HdfsNetworkException);
}

TEST_F(TestSocket, ConnectFailure_Fcntl) {
    struct addrinfo addr;
    TcpSocketImpl sock;
    EXPECT_CALL(syscall, socket(_, _, _)).Times(2).WillRepeatedly(Return(1));
    EXPECT_CALL(syscall, close(_)).Times(2).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, shutdown(_, _)).Times(2).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, setsockopt(_,_,_,_,_)).Times(AnyNumber()).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, fcntl(_, _, _)).Times(3).WillOnce(Return(-1)).WillOnce(
        Return(0)).WillOnce(Return(-1));
    EXPECT_THROW(sock.connect(&addr, "host", "port", 1000),
                 HdfsNetworkException);
    EXPECT_THROW(sock.connect(&addr, "host", "port", 1000),
                 HdfsNetworkException);
}

#ifdef SO_NOSIGPIPE
TEST_F(TestSocket, ConnectFailure_Setsockopt) {
    struct addrinfo addr;
    TcpSocketImpl sock;
    EXPECT_CALL(syscall, socket(_, _, _)).Times(1).WillRepeatedly(Return(1));
    EXPECT_CALL(syscall, close(_)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, shutdown(_, _)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, fcntl(_, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(0));
    EXPECT_CALL(syscall, setsockopt(_, _, _, _, _)).Times(1).WillOnce(
        Return(-1));
    EXPECT_THROW(sock.connect(&addr, "host", "port", 1000),
                 HdfsNetworkException);
}
#endif

TEST_F(TestSocket, ConnectFailure_ConnectRefused) {
    struct addrinfo addr;
    TcpSocketImpl sock;
    EXPECT_CALL(syscall, socket(_, _, _)).Times(1).WillRepeatedly(Return(1));
    EXPECT_CALL(syscall, close(_)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, shutdown(_, _)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, fcntl(_, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(0));
    EXPECT_CALL(syscall, setsockopt(_, _, _, _, _)).Times(AnyNumber()).WillOnce(
        Return(0));
    EXPECT_CALL(syscall, connect(_, _, _)).Times(1).WillOnce(
        SetErrnoAndReturn(ECONNREFUSED, -1));
    EXPECT_THROW(sock.connect(&addr, "host", "port", 1000),
                 HdfsNetworkException);
}

TEST_F(TestSocket, ConnectFailure_ConnectInterupted) {
    struct addrinfo addr;
    TcpSocketImpl sock;
    MockOperationCanceledCallback * mockCancelObj =
        new MockOperationCanceledCallback;
    MockCancelObject * mockCancel = mockCancelObj;
    EXPECT_CALL(syscall, socket(_, _, _)).Times(2).WillRepeatedly(Return(1));
    EXPECT_CALL(syscall, close(_)).Times(2).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, shutdown(_, _)).Times(2).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, fcntl(_, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(0));
    EXPECT_CALL(syscall, setsockopt(_, _, _, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(0));
    EXPECT_CALL(syscall, connect(_, _, _)).Times(3).WillOnce(
        SetErrnoAndReturn(EINTR, -1)).WillOnce(SetErrnoAndReturn(EINTR, -1)).WillOnce(
            SetErrnoAndReturn(ECONNREFUSED, -1));
    EXPECT_CALL(*mockCancelObj, check()).Times(2).WillOnce(Return(true)).WillOnce(
        Return(false));
    ChecnOperationCanceledCallback = bind(&MockCancelObject::canceled,
                                          mockCancel);
    EXPECT_THROW(sock.connect(&addr, "host", "port", 1000), HdfsCanceled);
    EXPECT_THROW(sock.connect(&addr, "host", "port", 1000),
                 HdfsNetworkException);
    delete mockCancelObj;
}

TEST_F(TestSocket, ConnectFailure_Poll) {
    struct addrinfo addr;
    TcpSocketImpl sock;
    EXPECT_CALL(syscall, socket(_, _, _)).Times(2).WillRepeatedly(Return(1));
    EXPECT_CALL(syscall, close(_)).Times(2).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, shutdown(_, _)).Times(2).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, fcntl(_, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(0));
    EXPECT_CALL(syscall, setsockopt(_, _, _, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(0));
    EXPECT_CALL(syscall, connect(_, _, _)).Times(2).WillRepeatedly(
        SetErrnoAndReturn(EINPROGRESS, -1));
    EXPECT_CALL(syscall, poll(_, _, _)).Times(2).WillOnce(
        SetErrnoAndReturn(EAGAIN, -1)).WillOnce(Return(0));
    EXPECT_THROW(sock.connect(&addr, "host", "port", 1000),
                 HdfsNetworkException);
    EXPECT_THROW(sock.connect(&addr, "host", "port", 1000), HdfsTimeoutException);
}

TEST_F(TestSocket, ConnectFailure_Timeout) {
    struct addrinfo addr;
    TcpSocketImpl sock;
    EXPECT_CALL(syscall, socket(_, _, _)).Times(2).WillRepeatedly(Return(1));
    EXPECT_CALL(syscall, close(_)).Times(2).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, shutdown(_, _)).Times(2).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, fcntl(_, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(0));
    EXPECT_CALL(syscall, setsockopt(_, _, _, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(0));
    EXPECT_CALL(syscall, connect(_, _, _)).Times(2).WillOnce(
        SetErrnoAndReturn(ETIMEDOUT, -1)).WillOnce(
            SetErrnoAndReturn(EINPROGRESS, -1));
    EXPECT_CALL(syscall, poll(_, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(1));
    EXPECT_CALL(syscall, getpeername(_, _, _)).Times(1).WillOnce(Return(-1));
    EXPECT_CALL(syscall, recv(_, _, _, _)).Times(1).WillOnce(
        SetErrnoAndReturn(ETIMEDOUT, -1));
    EXPECT_THROW(sock.connect(&addr, "host", "port", 1000), HdfsTimeoutException);
    EXPECT_THROW(sock.connect(&addr, "host", "port", 1000), HdfsTimeoutException);
}

TEST_F(TestSocket, ConnectFailure_ConnectFailed) {
    struct addrinfo addr;
    TcpSocketImpl sock;
    EXPECT_CALL(syscall, socket(_, _, _)).Times(1).WillRepeatedly(Return(1));
    EXPECT_CALL(syscall, close(_)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, shutdown(_, _)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, fcntl(_, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(0));
    EXPECT_CALL(syscall, setsockopt(_, _, _, _, _)).Times(AnyNumber()).WillOnce(
        Return(0));
    EXPECT_CALL(syscall, connect(_, _, _)).Times(1).WillOnce(
        SetErrnoAndReturn(EINPROGRESS, -1));
    EXPECT_CALL(syscall, poll(_, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(1));
    EXPECT_CALL(syscall, getpeername(_, _, _)).Times(1).WillOnce(Return(-1));
    EXPECT_CALL(syscall, recv(_, _, _, _)).Times(1).WillOnce(
        SetErrnoAndReturn(ECONNREFUSED, -1));
    EXPECT_THROW(sock.connect(&addr, "host", "port", 1000),
                 HdfsNetworkException);
}

TEST_F(TestSocket, ConnectSuccess) {
    struct addrinfo addr;
    TcpSocketImpl sock;
    EXPECT_CALL(syscall, socket(_, _, _)).Times(1).WillRepeatedly(Return(1));
    EXPECT_CALL(syscall, close(_)).Times(1);
    EXPECT_CALL(syscall, shutdown(_, _)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, fcntl(_, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(0));
    EXPECT_CALL(syscall, setsockopt(_, _, _, _, _)).Times(AnyNumber()).WillOnce(
        Return(0));
    EXPECT_CALL(syscall, connect(_, _, _)).Times(1).WillOnce(
        SetErrnoAndReturn(EINPROGRESS, -1));
    EXPECT_CALL(syscall, poll(_, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(1));
    EXPECT_CALL(syscall, getpeername(_, _, _)).Times(1).WillOnce(Return(0));
    EXPECT_CALL(syscall, recv(_, _, _, _)).Times(0);
    EXPECT_NO_THROW(sock.connect(&addr, "host", "port", 1000));
}

TEST_F(TestSocket, ConnectMultiAddrFailure_Getaddrinfo) {
    TcpSocketImpl sock;
    EXPECT_CALL(syscall, getaddrinfo(_, _, _, _)).Times(1).WillOnce(
        Return(EAI_MEMORY));
    EXPECT_CALL(syscall, freeaddrinfo(_)).Times(0);
    EXPECT_THROW(sock.connect("host", "port", 1000), HdfsNetworkException);
}

TEST_F(TestSocket, ConnectMultiAddrFailure_ThrowLastError) {
    TcpSocketImpl sock;
    struct addrinfo addr[2];
    addr[0].ai_next = &addr[1];
    addr[1].ai_next = NULL;
    EXPECT_CALL(syscall, getaddrinfo(_, _, _, _)).Times(1).WillOnce(
        DoAll(SetArgPointee<3>(&addr[0]), Return(0)));
    EXPECT_CALL(syscall, freeaddrinfo(_)).Times(1).WillOnce(Return());
    EXPECT_CALL(syscall, socket(_, _, _)).Times(1).WillRepeatedly(
        SetErrnoAndReturn(ENFILE, -1));
    EXPECT_THROW(sock.connect("host", "port", 1000), HdfsNetworkException);
}

TEST_F(TestSocket, ConnectMultiAddrFailure_Canceled) {
    TcpSocketImpl sock;
    struct addrinfo addr[2];
    addr[0].ai_next = &addr[1];
    addr[1].ai_next = NULL;
    MockOperationCanceledCallback * mockCancelObj =
        new MockOperationCanceledCallback;
    MockCancelObject * mockCancel = mockCancelObj;
    EXPECT_CALL(syscall, getaddrinfo(_, _, _, _)).Times(1).WillOnce(
        DoAll(SetArgPointee<3>(&addr[0]), Return(0)));
    EXPECT_CALL(syscall, freeaddrinfo(_)).Times(1).WillOnce(Return());
    EXPECT_CALL(*mockCancelObj, check()).Times(1).WillOnce(Return(true));
    ChecnOperationCanceledCallback = bind(&MockCancelObject::canceled,
                                          mockCancel);
    EXPECT_THROW(sock.connect("host", "port", 1000), HdfsCanceled);
    delete mockCancelObj;
}

TEST_F(TestSocket, ConnectMultiAddrFailure_Timeout) {
    TcpSocketImpl sock;
    struct addrinfo addr[2];
    addr[0].ai_next = &addr[1];
    addr[1].ai_next = NULL;
    EXPECT_CALL(syscall, getaddrinfo(_, _, _, _)).Times(2).WillRepeatedly(
        DoAll(SetArgPointee<3>(&addr[0]), Return(0)));
    EXPECT_CALL(syscall, freeaddrinfo(_)).Times(2).WillRepeatedly(Return());
    EXPECT_CALL(syscall, socket(_, _, _)).Times(3).WillRepeatedly(Return(1));
    EXPECT_CALL(syscall, fcntl(_, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(0));
    EXPECT_CALL(syscall, setsockopt(_, _, _, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(0));
    EXPECT_CALL(syscall, poll(_, _, _)).Times(AnyNumber()).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, close(_)).Times(3);
    EXPECT_CALL(syscall, shutdown(_, _)).Times(3).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, connect(_, _, _)).Times(3).WillRepeatedly(
        SetErrnoAndReturn(EINPROGRESS, -1));
    EXPECT_THROW(sock.connect("host", "port", 0), HdfsTimeoutException);
    sock.close();
    EXPECT_THROW(sock.connect("host", "port", 900), HdfsTimeoutException);
}

TEST_F(TestSocket, ConnectMultiAddrSuccessfully) {
    TcpSocketImpl sock;
    EXPECT_CALL(syscall, socket(_, _, _)).Times(1).WillRepeatedly(Return(1));
    EXPECT_CALL(syscall, close(_)).Times(1);
    EXPECT_CALL(syscall, shutdown(_, _)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, fcntl(_, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(0));
    EXPECT_CALL(syscall, setsockopt(_, _, _, _, _)).Times(AnyNumber()).WillOnce(
        Return(0));
    EXPECT_CALL(syscall, connect(_, _, _)).Times(1).WillOnce(
        SetErrnoAndReturn(EINPROGRESS, -1));
    EXPECT_CALL(syscall, poll(_, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(1));
    EXPECT_CALL(syscall, getpeername(_, _, _)).Times(1).WillOnce(Return(0));
    EXPECT_CALL(syscall, recv(_, _, _, _)).Times(0);
    struct addrinfo addr[2];
    addr[0].ai_next = &addr[1];
    addr[1].ai_next = NULL;
    EXPECT_CALL(syscall, getaddrinfo(_, _, _, _)).Times(1).WillRepeatedly(
        DoAll(SetArgPointee<3>(&addr[0]), Return(0)));
    EXPECT_CALL(syscall, freeaddrinfo(_)).Times(1).WillRepeatedly(Return());
    EXPECT_NO_THROW(sock.connect("host", "port", 0));
}

TEST_F(TestSocket, ConnectMultiAddrFinallySuccess) {
    TcpSocketImpl sock;
    EXPECT_CALL(syscall, socket(_, _, _)).Times(2).WillRepeatedly(Return(1));
    EXPECT_CALL(syscall, close(_)).Times(2);
    EXPECT_CALL(syscall, shutdown(_, _)).Times(2).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, fcntl(_, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(0));
    EXPECT_CALL(syscall, setsockopt(_, _, _, _, _)).Times(AnyNumber()).WillRepeatedly(
        Return(0));
    EXPECT_CALL(syscall, connect(_, _, _)).Times(2).WillRepeatedly(
        SetErrnoAndReturn(EINPROGRESS, -1));
    EXPECT_CALL(syscall, poll(_, _, _)).Times(AnyNumber()).WillOnce(
        Return(0)).WillRepeatedly(Return(1));
    EXPECT_CALL(syscall, getpeername(_, _, _)).Times(1).WillOnce(Return(0));
    struct addrinfo addr[2];
    addr[0].ai_next = &addr[1];
    addr[1].ai_next = NULL;
    EXPECT_CALL(syscall, getaddrinfo(_, _, _, _)).Times(1).WillRepeatedly(
        DoAll(SetArgPointee<3>(&addr[0]), Return(0)));
    EXPECT_CALL(syscall, freeaddrinfo(_)).Times(1).WillRepeatedly(Return());
    EXPECT_NO_THROW(sock.connect("host", "port", 1000));
}

TEST_F(TestSocket, ReadFailure) {
    TcpSocketImpl sock;
    sock.sock = 1;
    char buffer[128];
    EXPECT_CALL(syscall, close(_)).Times(1);
    EXPECT_CALL(syscall, shutdown(_, _)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, recv(_, _, _, _)).Times(3).WillOnce(
        SetErrnoAndReturn(EINTR, -1)).WillOnce(SetErrnoAndReturn(EBADF, -1)).WillOnce(
            Return(0));
    EXPECT_THROW(sock.read(buffer, sizeof(buffer)), HdfsNetworkException);
    EXPECT_THROW(sock.read(buffer, sizeof(buffer)), HdfsEndOfStream);
}

TEST_F(TestSocket, ReadSuccess) {
    TcpSocketImpl sock;
    sock.sock = 1;
    char buffer[128];
    EXPECT_CALL(syscall, close(_)).Times(1);
    EXPECT_CALL(syscall, shutdown(_, _)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, recv(_, _, _, _)).Times(1).WillOnce(
        Return(sizeof(buffer)));
    EXPECT_EQ(static_cast<int32_t>(sizeof(buffer)), sock.read(buffer, sizeof(buffer)));
}

TEST_F(TestSocket, ReadFullyFailure) {
    TcpSocketImpl sock;
    sock.sock = 1;
    char buffer[128];
    EXPECT_CALL(syscall, close(_)).Times(1);
    EXPECT_CALL(syscall, shutdown(_, _)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, poll(_, _, _)).Times(2).WillOnce(Return(0)).WillOnce(
        Invoke(bind(&InvokeWaitAndReturn<int>, 1000, 0, 0)));
    EXPECT_CALL(syscall, recv(_, _, _, _)).Times(0);
    EXPECT_THROW(sock.readFully(buffer, sizeof(buffer), 0), HdfsTimeoutException);
    EXPECT_THROW(sock.readFully(buffer, sizeof(buffer), 500), HdfsTimeoutException);
}

TEST_F(TestSocket, ReadFullySuccess) {
    TcpSocketImpl sock;
    sock.sock = 1;
    char buffer[128];
    char target[] = "hello world";
    int32_t tsize = sizeof(target) / 2;
    void * t1 = target, *t2 = target + tsize;
    EXPECT_CALL(syscall, close(_)).Times(1);
    EXPECT_CALL(syscall, shutdown(_, _)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, poll(_, _, _)).Times(2).WillRepeatedly(Return(1));
    EXPECT_CALL(syscall, recv(_, _, _, _)).Times(2).WillOnce(
        Invoke(bind(&ReadAction, _1, _2, _3, _4, t1, tsize))).WillOnce(
            Invoke(bind(&ReadAction, _1, _2, _3, _4, t2, tsize)));
    memset(buffer, 0, sizeof(buffer));
    EXPECT_NO_THROW(sock.readFully(buffer, sizeof(target), 500));
    EXPECT_STREQ(target, buffer);
}

TEST_F(TestSocket, WriteFailure) {
    TcpSocketImpl sock;
    sock.sock = 1;
    char buffer[128];
    EXPECT_CALL(syscall, close(_)).Times(1);
    EXPECT_CALL(syscall, shutdown(_, _)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, send(_, _, _, _)).Times(2).WillOnce(
        SetErrnoAndReturn(EINTR, -1)).WillOnce(
            SetErrnoAndReturn(EBADF, -1));
    EXPECT_THROW(sock.write(buffer, sizeof(buffer)), HdfsNetworkException);
}

TEST_F(TestSocket, WriteSuccess) {
    TcpSocketImpl sock;
    sock.sock = 1;
    char buffer[128];
    EXPECT_CALL(syscall, close(_)).Times(1);
    EXPECT_CALL(syscall, shutdown(_, _)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, send(_, _, _, _)).Times(1).WillOnce(
        Return(sizeof(buffer)));
    EXPECT_EQ(static_cast<int32_t>(sizeof(buffer)), sock.write(buffer, sizeof(buffer)));
}

TEST_F(TestSocket, WriteFullyFailure) {
    TcpSocketImpl sock;
    sock.sock = 1;
    char buffer[128];
    EXPECT_CALL(syscall, close(_)).Times(1);
    EXPECT_CALL(syscall, shutdown(_, _)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, poll(_, _, _)).Times(2).WillOnce(Return(0)).WillOnce(
        Invoke(bind(&InvokeWaitAndReturn<int>, 1000, 0, 0)));
    EXPECT_CALL(syscall, send(_, _, _, _)).Times(0);
    EXPECT_THROW(sock.writeFully(buffer, sizeof(buffer), 0), HdfsTimeoutException);
    EXPECT_THROW(sock.writeFully(buffer, sizeof(buffer), 500), HdfsTimeoutException);
}

TEST_F(TestSocket, WriteFullySuccess) {
    TcpSocketImpl sock;
    sock.sock = 1;
    char buffer[128];
    char target[] = "hello world";
    int32_t tsize = (sizeof(target) + 1) / 2;
    void * b1 = buffer, *b2 = buffer + tsize;
    EXPECT_CALL(syscall, close(_)).Times(1);
    EXPECT_CALL(syscall, shutdown(_, _)).Times(1).WillRepeatedly(Return(0));
    EXPECT_CALL(syscall, poll(_, _, _)).Times(2).WillRepeatedly(Return(1));
    EXPECT_CALL(syscall, send(_, _, _, _)).Times(2).WillOnce(
        Invoke(bind(&WriteAction, _1, _2, _3, _4, b1, tsize))).WillOnce(
            Invoke(bind(&WriteAction, _1, _2, _3, _4, b2, tsize)));
    memset(buffer, 0, sizeof(buffer));
    EXPECT_NO_THROW(sock.writeFully(target, sizeof(target), 500));
    EXPECT_STREQ(target, buffer);
}
