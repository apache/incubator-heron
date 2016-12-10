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
#ifndef _HDFS_LIBHDFS3_MOCK_MOCKSOCKCALL_H_
#define _HDFS_LIBHDFS3_MOCK_MOCKSOCKCALL_H_

#include "gmock/gmock.h"

#include "MockSystem.h"

namespace Hdfs {
namespace Mock {

class MockSockSysCall: public MockSockSysCallInterface {
public:
    MOCK_METHOD4(recv , ssize_t (int sock, void * buffer, size_t size, int flag));
    MOCK_METHOD4(send , ssize_t (int sock, const void * buffer, size_t size,
                    int flag));
    MOCK_METHOD3(recvmsg , ssize_t (int socket, struct msghdr *message, int flags));
    MOCK_METHOD4(getaddrinfo , int (const char * __restrict host,
                    const char * __restrict port,
                    const struct addrinfo * __restrict hint,
                    struct addrinfo ** __restrict addr));
    MOCK_METHOD1(freeaddrinfo , void (struct addrinfo * addr));
    MOCK_METHOD3(socket , int (int family, int type, int protocol));
    MOCK_METHOD3(connect , int (int sock, const struct sockaddr * addr,
                    socklen_t len));
    MOCK_METHOD3(getpeername , int (int sock, struct sockaddr * __restrict peer,
                    socklen_t * __restrict len));
    MOCK_METHOD3(fcntl , int (int sock, int flag, int value));
    MOCK_METHOD5(setsockopt , int (int sock, int level, int optname, const void *optval,
                    socklen_t optlen));
    MOCK_METHOD3(poll , int (struct pollfd * pfd, nfds_t size, int timeout));
    MOCK_METHOD2(shutdown , int (int sock, int how));
    MOCK_METHOD1(close , int (int sock));
};

}
}

#endif /* _HDFS_LIBHDFS3_MOCK_MOCKSOCKCALL_H_ */
