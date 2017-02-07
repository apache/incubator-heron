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
#ifndef _HDFS_LIBHDFS3_MOCK_MOCKSYSTEM_H_
#define _HDFS_LIBHDFS3_MOCK_MOCKSYSTEM_H_

#include <poll.h>

#include <unistd.h>
#include <fcntl.h>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netdb.h>

namespace Hdfs {
namespace Mock {

class MockSockSysCallInterface {
public:
    virtual ~MockSockSysCallInterface() {
    }

    virtual ssize_t recv(int sock, void * buffer, size_t size, int flag) = 0;
    virtual ssize_t send(int sock, const void * buffer, size_t size,
            int flag) = 0;
  virtual ssize_t recvmsg(int socket, struct msghdr *message, int flags) = 0;
    virtual int getaddrinfo(const char * __restrict host,
            const char * __restrict port,
            const struct addrinfo * __restrict hint,
            struct addrinfo ** __restrict addr) = 0;
    virtual void freeaddrinfo(struct addrinfo * addr) = 0;
    virtual int socket(int family, int type, int protocol) = 0;
    virtual int connect(int sock, const struct sockaddr * addr,
            socklen_t len) = 0;
    virtual int getpeername(int sock, struct sockaddr * __restrict peer,
            socklen_t * __restrict len) = 0;
    virtual int fcntl(int sock, int flag, int value) = 0;
    virtual int setsockopt(int sock, int level, int optname, const void *optval,
            socklen_t optlen) = 0;
    virtual int poll(struct pollfd * pfd, nfds_t size, int timeout) = 0;
    virtual int shutdown(int sock, int how) = 0;
    virtual int close(int sock) = 0;
};

}
}

namespace MockSystem {

extern Hdfs::Mock::MockSockSysCallInterface * MockSockSysCallObj;

ssize_t recv(int sock, void * buffer, size_t size, int flag);

ssize_t send(int sock, const void * buffer, size_t size, int flag);

ssize_t recvmsg(int socket, struct msghdr *message, int flags);

int getaddrinfo(const char * __restrict host, const char * __restrict port,
        const struct addrinfo * __restrict hint,
        struct addrinfo ** __restrict addr);

void freeaddrinfo(struct addrinfo * addr);

int socket(int family, int type, int protocol);

int connect(int sock, const struct sockaddr * addr, socklen_t len);

int getpeername(int sock, struct sockaddr * __restrict peer,
        socklen_t * __restrict len);

int fcntl(int sock, int flag, int value);

int setsockopt(int sock, int level, int optname, const void *optval,
        socklen_t optlen);

int poll(struct pollfd * pfd, nfds_t size, int timeout);

int shutdown(int sock, int how);

int close(int sock);

}

#endif /* _HDFS_LIBHDFS3_MOCK_MOCKSYSTEM_H_ */
