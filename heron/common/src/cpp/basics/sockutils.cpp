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

#include "basics/sockutils.h"
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include "glog/logging.h"
#include "config/heron-config.h"
#include "basics/sprcodes.h"
#include "basics/spconsts.h"

sp_int32 SockUtils::setNonBlocking(sp_int32 fd) {
  sp_int32 flags;
  if ((flags = ::fcntl(fd, F_GETFL, 0)) < 0) return SP_NOTOK;

  if (::fcntl(fd, F_SETFL, flags | O_NONBLOCK) < 0) return SP_NOTOK;

  return SP_OK;
}

sp_int32 SockUtils::getSendBufferSize(sp_int32 fd, sp_int32 &size) {
  socklen_t optlen = sizeof(sp_int32);
  return ::getsockopt(fd, SOL_SOCKET, SO_SNDBUF, &size, &optlen);
}

sp_int32 SockUtils::setSendBufferSize(sp_int32 fd, sp_int32 size) {
  return ::setsockopt(fd, SOL_SOCKET, SO_SNDBUF, reinterpret_cast<char *>(&size), sizeof(size));
}

sp_int32 SockUtils::getRecvBufferSize(sp_int32 fd, sp_int32 &size) {
  socklen_t optlen = sizeof(sp_int32);
  return ::getsockopt(fd, SOL_SOCKET, SO_RCVBUF, &size, &optlen);
}

sp_int32 SockUtils::setRecvBufferSize(sp_int32 fd, sp_int32 size) {
  return ::setsockopt(fd, SOL_SOCKET, SO_RCVBUF, reinterpret_cast<char *>(&size), sizeof(size));
}

sp_int32 SockUtils::setKeepAlive(sp_int32 fd) {
  sp_int32 alive = 1;
  return ::setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, reinterpret_cast<char *>(&alive),
                      sizeof(alive));
}

sp_int32 SockUtils::setReuseAddress(sp_int32 fd) {
  sp_int32 sopt = 1;
  return ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<char *>(&sopt), sizeof(sopt));
}

sp_int32 SockUtils::setKeepIdleTime(sp_int32 fd, sp_int32 time) {
#if defined(IS_MACOSX)
  sp_int32 on = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof(on)) < 0) return SP_NOTOK;
  return ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE, reinterpret_cast<char *>(&time),
                      sizeof(time));
#else
  return ::setsockopt(fd, SOL_TCP, TCP_KEEPIDLE, reinterpret_cast<char *>(&time), sizeof(time));
#endif
}

sp_int32 SockUtils::setKeepIdleCount(sp_int32 fd, sp_int32 count) {
#if defined(IS_MACOSX)
  return ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, reinterpret_cast<char *>(&count),
                      sizeof(count));
#else
  return ::setsockopt(fd, SOL_TCP, TCP_KEEPCNT, reinterpret_cast<char *>(&count), sizeof(count));
#endif
}

sp_int32 SockUtils::setKeepIdleInterval(sp_int32 fd, sp_int32 interval) {
#if defined(IS_MACOSX)
  return ::setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, reinterpret_cast<char *>(&interval),
                      sizeof(interval));
#else
  return ::setsockopt(fd, SOL_TCP, TCP_KEEPINTVL, reinterpret_cast<char *>(&interval),
                      sizeof(interval));
#endif
}

sp_int32 SockUtils::setKeepIdleParams(sp_int32 fd, sp_int32 time, sp_int32 count,
                                      sp_int32 interval) {
  if (SockUtils::setKeepIdleTime(fd, time) < 0) {
    PLOG(ERROR) << "unable to set keep idle time ";
    return SP_NOTOK;
  }

  if (SockUtils::setKeepIdleCount(fd, count) < 0) {
    PLOG(ERROR) << "unable to set keep idle count ";
    return SP_NOTOK;
  }

  if (SockUtils::setKeepIdleInterval(fd, interval) < 0) {
    PLOG(ERROR) << "unable to set keep idle interval ";
    return SP_NOTOK;
  }

  return SP_OK;
}

sp_int32 SockUtils::setTcpNoDelay(sp_int32 fd) {
  sp_int32 on = 1;
  return ::setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, reinterpret_cast<char *>(&on), sizeof(on));
}

sp_int32 SockUtils::setSocketDefaults(sp_int32 fd) {
  // set the max send buffer size so that it makes the pipe fatter
  sp_int32 send_buff;

  // get the send buffer size
  auto res = SockUtils::getSendBufferSize(fd, send_buff);
  if (res == SP_NOTOK) {
    PLOG(ERROR) << "Could not get the max SO_SNDBUF size";
  } else {
    // Set to a dummy high value. Linux will fall back to max. OSX will throw an error.
    send_buff *= 100;
    res = SockUtils::setSendBufferSize(fd, send_buff);
    if (res == SP_NOTOK) {
      PLOG(ERROR) << "Could not set SO_SNDBUF to max " << send_buff;
    }
  }

  // set the max recv buffer size so that it makes the pipe fatter

  // get the recv buffer size
  sp_int32 recv_buff;
  res = SockUtils::getRecvBufferSize(fd, recv_buff);
  if (res == SP_NOTOK) {
    PLOG(ERROR) << "Could not get the max SO_RCVBUF size";
  } else {
    // Set to a dummy high value. Linux will fall back to max. OSX will throw an error.
    recv_buff *= 100;
    res = SockUtils::setRecvBufferSize(fd, recv_buff);
    if (res == SP_NOTOK) {
      PLOG(ERROR) << "Could not set SO_RCVBUF to max " << recv_buff;
    }
  }

  // make it non blocking
  if (SockUtils::setNonBlocking(fd) < 0) {
    PLOG(ERROR) << "unable to make socket non blocking";
    return SP_NOTOK;
  }

  // enable keepalive for this socket
  if (SockUtils::setKeepAlive(fd) < 0) {
    PLOG(ERROR) << "setsockopt for keepalive failed in server";
    return SP_NOTOK;
  }

  // set a reasonable keepalive
  sp_int32 ka_idle = constTcpKeepAliveSecs;
  sp_int32 ka_interval = constTcpKeepAliveProbeInterval;
  sp_int32 ka_nprobes = constTcpKeepAliveProbes;

  if (SockUtils::setKeepIdleParams(fd, ka_idle, ka_nprobes, ka_interval) < 0) {
    PLOG(ERROR) << "setsockopt for keepalive failed ";
    return SP_NOTOK;
  }

  if (SockUtils::setTcpNoDelay(fd)) {
    PLOG(ERROR) << "setting tcp_nodelay failed ";
    return SP_NOTOK;
  }

  return SP_OK;
}
