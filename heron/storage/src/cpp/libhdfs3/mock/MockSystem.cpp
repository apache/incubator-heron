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
#include <cassert>
#include <climits>
#include <errno.h>
#include <poll.h>

#include <unistd.h>
#include <fcntl.h>

#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netdb.h>

#include <string.h>

#include "MockSystem.h"

namespace MockSystem {

Hdfs::Mock::MockSockSysCallInterface * MockSockSysCallObj = NULL;

ssize_t recv(int sock, void * buffer, size_t size, int flag) {
    if (MockSockSysCallObj) {
        return MockSockSysCallObj->recv(sock, buffer, size, flag);
    }
    return ::recv(sock, buffer, size, flag);
}

ssize_t send(int sock, const void * buffer, size_t size, int flag) {
    if (MockSockSysCallObj) {
        return MockSockSysCallObj->send(sock, buffer, size, flag);
    }
    return ::send(sock, buffer, size, flag);
}

ssize_t recvmsg(int socket, struct msghdr *message, int flags) {
    if (MockSockSysCallObj) {
        return MockSockSysCallObj->recvmsg(socket, message, flags);
    }
    return ::recvmsg(socket, message, flags);
}

int getaddrinfo(const char * __restrict host, const char * __restrict port,
        const struct addrinfo * __restrict hint,
        struct addrinfo ** __restrict addr) {
    if (MockSockSysCallObj) {
        return MockSockSysCallObj->getaddrinfo(host, port, hint, addr);
    }
    return ::getaddrinfo(host, port, hint, addr);
}

void freeaddrinfo(struct addrinfo * addr) {
    if (MockSockSysCallObj) {
        MockSockSysCallObj->freeaddrinfo(addr);
    } else {
        ::freeaddrinfo(addr);
    }
}

int socket(int family, int type, int protocol) {
    if (MockSockSysCallObj) {
        return MockSockSysCallObj->socket(family, type, protocol);
    }
    return ::socket(family, type, protocol);
}

int connect(int sock, const struct sockaddr * addr, socklen_t len) {
    if (MockSockSysCallObj) {
        return MockSockSysCallObj->connect(sock, addr, len);
    }
    return ::connect(sock, addr, len);
}

int getpeername(int sock, struct sockaddr * __restrict peer,
        socklen_t * __restrict len) {
    if (MockSockSysCallObj) {
        return MockSockSysCallObj->getpeername(sock, peer, len);
    }
    return ::getpeername(sock, peer, len);
}

int fcntl(int sock, int flag, int value) {
    if (MockSockSysCallObj) {
        return MockSockSysCallObj->fcntl(sock, flag, value);
    }
    return ::fcntl(sock, flag, value);
}

int setsockopt(int sock, int level, int optname, const void *optval,
        socklen_t optlen) {
    if (MockSockSysCallObj) {
        return MockSockSysCallObj->setsockopt(sock, level, optname, optval,
                optlen);
    }
    return ::setsockopt(sock, level, optname, optval, optlen);
}

int poll(struct pollfd * pfd, nfds_t size, int timeout) {
    if (MockSockSysCallObj) {
        return MockSockSysCallObj->poll(pfd, size, timeout);
    }
    return ::poll(pfd, size, timeout);
}

int shutdown(int sock, int how) {
    if (MockSockSysCallObj) {
        return MockSockSysCallObj->shutdown(sock, how);
    }
    return ::shutdown(sock, how);
}

int close(int sock) {
    if (MockSockSysCallObj) {
        return MockSockSysCallObj->close(sock);
    }
    return ::close(sock);
}

}
