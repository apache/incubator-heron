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

#include "network/TcpSocket.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <unistd.h>

#include <cassert>
#include <climits>
#include <cstring>
#include <chrono>
#include <sstream>

#include "common/DateTime.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "network/Syscall.h"

using std::chrono::steady_clock;

namespace Hdfs {
namespace Internal {

TcpSocketImpl::TcpSocketImpl() :
    sock(-1), lingerTimeout(-1) {
}

TcpSocketImpl::~TcpSocketImpl() {
    close();
}

int32_t TcpSocketImpl::read(char * buffer, int32_t size) {
    assert(-1 != sock);
    assert(NULL != buffer && size > 0);
    int32_t rc;

    do {
        rc = HdfsSystem::recv(sock, buffer, size, 0);
    } while (-1 == rc && EINTR == errno && !CheckOperationCanceled());

    if (-1 == rc) {
        THROW(HdfsNetworkException, "Read %d bytes failed from %s: %s",
              size, remoteAddr.c_str(), GetSystemErrorInfo(errno));
    }

    if (0 == rc) {
        THROW(HdfsEndOfStream, "Read %d bytes failed from %s: End of the stream",
              size, remoteAddr.c_str());
    }

    return rc;
}

void TcpSocketImpl::readFully(char * buffer, int32_t size, int timeout) {
    assert(-1 != sock);
    assert(NULL != buffer && size > 0);
    int32_t todo = size, rc;
    int deadline = timeout;

    while (todo > 0) {
        steady_clock::time_point s = steady_clock::now();
        CheckOperationCanceled();

        if (poll(true, false, deadline)) {
            rc = read(buffer + (size - todo), todo);
            todo -= rc;
        }

        steady_clock::time_point e = steady_clock::now();

        if (timeout > 0) {
            deadline -= ToMilliSeconds(s, e);
        }

        if (todo > 0 && timeout >= 0 && deadline <= 0) {
            THROW(HdfsTimeoutException, "Read %d bytes timeout from %s", size, remoteAddr.c_str());
        }
    }
}

int32_t TcpSocketImpl::write(const char * buffer, int32_t size) {
    assert(-1 != sock);
    assert(NULL != buffer && size > 0);
    int32_t rc;

    do {
#ifdef IS_LINUX  // on linux
        rc = HdfsSystem::send(sock, buffer, size, MSG_NOSIGNAL);
#else
        rc = HdfsSystem::send(sock, buffer, size, 0);
#endif
    } while (-1 == rc && EINTR == errno && !CheckOperationCanceled());

    if (-1 == rc) {
        THROW(HdfsNetworkException, "Write %d bytes failed to %s: %s",
              size, remoteAddr.c_str(), GetSystemErrorInfo(errno));
    }

    return rc;
}

void TcpSocketImpl::writeFully(const char * buffer, int32_t size, int timeout) {
    assert(-1 != sock);
    assert(NULL != buffer && size > 0);
    int32_t todo = size, rc;
    int deadline = timeout;

    while (todo > 0) {
        steady_clock::time_point s = steady_clock::now();
        CheckOperationCanceled();

        if (poll(false, true, deadline)) {
            rc = write(buffer + (size - todo), todo);
            todo -= rc;
        }

        steady_clock::time_point e = steady_clock::now();

        if (timeout > 0) {
            deadline -= ToMilliSeconds(s, e);
        }

        if (todo > 0 && timeout >= 0 && deadline <= 0) {
            THROW(HdfsTimeoutException, "Write %d bytes timeout to %s", size, remoteAddr.c_str());
        }
    }
}

void TcpSocketImpl::connect(const char * host, int port, int timeout) {
    std::stringstream ss;
    ss.imbue(std::locale::classic());
    ss << port;
    connect(host, ss.str().c_str(), timeout);
}

void TcpSocketImpl::connect(const char * host, const char * port, int timeout) {
    assert(-1 == sock);
    struct addrinfo hints, *addrs, *paddr;
    memset(&hints, 0, sizeof(hints));
    hints.ai_family = PF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    int retval = HdfsSystem::getaddrinfo(host, port, &hints, &addrs);

    if (0 != retval) {
        THROW(HdfsNetworkConnectException, "Failed to resolve address \"%s:%s\" %s",
              host, port, gai_strerror(retval));
    }

    int deadline = timeout;
    std::stringstream ss;
    ss.imbue(std::locale::classic());
    ss << "\"" << host << ":" << port << "\"";
    remoteAddr = ss.str();

    try {
        for (paddr = addrs; NULL != paddr; paddr = paddr->ai_next) {
            steady_clock::time_point s = steady_clock::now();
            CheckOperationCanceled();

            try {
                connect(paddr, host, port, deadline);
            } catch (HdfsNetworkConnectException & e) {
                if (NULL == paddr->ai_next) {
                    throw;
                }
            } catch (HdfsTimeoutException & e) {
                if (NULL == paddr->ai_next) {
                    throw;
                }
            }

            if (-1 != sock) {
                HdfsSystem::freeaddrinfo(addrs);
                return;
            }

            steady_clock::time_point e = steady_clock::now();

            if (timeout > 0) {
                deadline -= ToMilliSeconds(s, e);
            }

            if (-1 == sock && timeout >= 0 && deadline <= 0) {
                THROW(HdfsTimeoutException, "Connect to \"%s:%s\" timeout", host, port);
            }
        }
    } catch (...) {
        HdfsSystem::freeaddrinfo(addrs);
        throw;
    }
}

void TcpSocketImpl::connect(struct addrinfo * paddr, const char * host,
                            const char * port, int timeout) {
    assert(-1 == sock);
    sock = HdfsSystem::socket(paddr->ai_family, paddr->ai_socktype,
                              paddr->ai_protocol);

    if (-1 == sock) {
        THROW(HdfsNetworkException,
              "Create socket failed when connect to %s: %s",
              remoteAddr.c_str(), GetSystemErrorInfo(errno));
    }

    if (lingerTimeout >= 0) {
        setLingerTimeoutInternal(lingerTimeout);
    }

#ifdef __linux__
    /*
     * on linux some kernel use SO_SNDTIMEO as connect timeout.
     * It is OK to set a very large value here since the user has its own timeout mechanism.
     */
    setSendTimeout(3600000);
#endif

    try {
        setBlockMode(false);
        disableSigPipe();
        int rc = 0;

        do {
            rc = HdfsSystem::connect(sock, paddr->ai_addr, paddr->ai_addrlen);
        } while (rc < 0 && EINTR == errno && !CheckOperationCanceled());

        if (rc < 0) {
            if (EINPROGRESS != errno && EWOULDBLOCK != errno) {
                if (ETIMEDOUT == errno) {
                    THROW(HdfsTimeoutException, "Connect to \"%s:%s\" timeout",
                          host, port);
                } else {
                    THROW(HdfsNetworkConnectException,
                          "Connect to \"%s:%s\" failed: %s",
                          host, port, GetSystemErrorInfo(errno));
                }
            }

            if (!poll(false, true, timeout)) {
                THROW(HdfsTimeoutException, "Connect to \"%s:%s\" timeout", host, port);
            }

            struct sockaddr peer;

            unsigned int len = sizeof(peer);

            memset(&peer, 0, sizeof(peer));

            if (HdfsSystem::getpeername(sock, &peer, &len)) {
                /*
                 * connect failed, find out the error info.
                 */
                char c;
                rc = HdfsSystem::recv(sock, &c, 1, 0);
                assert(rc < 0);

                if (ETIMEDOUT == errno) {
                    THROW(HdfsTimeoutException, "Connect to \"%s:%s\" timeout",
                          host, port);
                }

                THROW(HdfsNetworkConnectException, "Connect to \"%s:%s\" failed: %s",
                      host, port, GetSystemErrorInfo(errno));
            }
        }

        setBlockMode(true);
    } catch (...) {
        close();
        throw;
    }
}

void TcpSocketImpl::setBlockMode(bool enable) {
    int flag;
    flag = HdfsSystem::fcntl(sock, F_GETFL, 0);

    if (-1 == flag) {
        THROW(HdfsNetworkException, "Get socket flag failed for remote node %s: %s",
              remoteAddr.c_str(), GetSystemErrorInfo(errno));
    }

    flag = enable ? (flag & ~O_NONBLOCK) : (flag | O_NONBLOCK);

    if (-1 == HdfsSystem::fcntl(sock, F_SETFL, flag)) {
        THROW(HdfsNetworkException, "Set socket flag failed for remote node %s: %s",
              remoteAddr.c_str(), GetSystemErrorInfo(errno));
    }
}

void TcpSocketImpl::disableSigPipe() {
#ifdef SO_NOSIGPIPE /* only available on macos*/
    int flag = 1;

    if (HdfsSystem::setsockopt(sock, SOL_SOCKET, SO_NOSIGPIPE, reinterpret_cast<char *>(&flag),
                               sizeof(flag))) {
        THROW(HdfsNetworkException, "Set socket flag failed for remote node %s: %s",
              remoteAddr.c_str(), GetSystemErrorInfo(errno));
    }

#endif
}

bool TcpSocketImpl::poll(bool read, bool write, int timeout) {
    assert(-1 != sock);
    int rc;
    struct pollfd pfd;

    do {
        memset(&pfd, 0, sizeof(pfd));
        pfd.fd = sock;

        if (read) {
            pfd.events |= POLLIN;
        }

        if (write) {
            pfd.events |= POLLOUT;
        }

        rc = HdfsSystem::poll(&pfd, 1, timeout);
    } while (-1 == rc && EINTR == errno && !CheckOperationCanceled());

    if (-1 == rc) {
        THROW(HdfsNetworkException, "Poll failed for remote node %s: %s",
              remoteAddr.c_str(), GetSystemErrorInfo(errno));
    }

    return 0 != rc;
}

void TcpSocketImpl::setNoDelay(bool enable) {
    assert(-1 != sock);
    int flag = enable ? 1 : 0;

    if (HdfsSystem::setsockopt(sock, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<char *>(&flag),
                               sizeof(flag))) {
        THROW(HdfsNetworkException, "Set socket flag failed for remote node %s: %s",
              remoteAddr.c_str(), GetSystemErrorInfo(errno));
    }
}

void TcpSocketImpl::setLingerTimeout(int timeout) {
    lingerTimeout = timeout;
}

void TcpSocketImpl::setLingerTimeoutInternal(int timeout) {
    assert(-1 != sock);
    struct linger l;
    l.l_onoff = timeout > 0 ? true : false;
    l.l_linger = timeout > 0 ? timeout : 0;

    if (HdfsSystem::setsockopt(sock, SOL_SOCKET, SO_LINGER, &l, sizeof(l))) {
        THROW(HdfsNetworkException, "Set socket flag failed for remote node %s: %s",
              remoteAddr.c_str(), GetSystemErrorInfo(errno));
    }
}

void TcpSocketImpl::setSendTimeout(int timeout) {
    assert(-1 != sock);
    struct timeval timeo;
    timeo.tv_sec = timeout / 1000;
    timeo.tv_usec = (timeout % 1000) * 1000;

    if (HdfsSystem::setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &timeo, sizeof(timeo))) {
        THROW(HdfsNetworkException, "Set socket flag failed for remote node %s: %s",
              remoteAddr.c_str(), GetSystemErrorInfo(errno));
    }
}

void TcpSocketImpl::close() {
    if (-1 != sock) {
        HdfsSystem::shutdown(sock, SHUT_RDWR);
        HdfsSystem::close(sock);
        sock = -1;
    }
}

}  // namespace Internal
}  // namespace Hdfs
