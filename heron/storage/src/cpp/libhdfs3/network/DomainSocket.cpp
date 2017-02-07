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

#include "network/DomainSocket.h"

#include <errno.h>
#include <netinet/in.h>
#include <stdint.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/un.h>
#include <unistd.h>

#include <cassert>
#include <cstddef>
#include <cstring>
#include <vector>

#include "common/DateTime.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "network/Syscall.h"

namespace Hdfs {
namespace Internal {

DomainSocketImpl::DomainSocketImpl() {}

DomainSocketImpl::~DomainSocketImpl() {}

void DomainSocketImpl::connect(const char *host, int port, int timeout) {
  connect(host, "", timeout);
}

void DomainSocketImpl::connect(const char *host, const char *port,
                               int timeout) {
  remoteAddr = host;
  assert(-1 == sock);
  sock = HdfsSystem::socket(AF_UNIX, SOCK_STREAM, 0);

  if (-1 == sock) {
    THROW(HdfsNetworkException, "Create socket failed when connect to %s: %s",
          remoteAddr.c_str(), GetSystemErrorInfo(errno));
  }

  try {
    int len, rc;
    disableSigPipe();
    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    rc = snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", host);

    if (rc < 0 || rc >= static_cast<int>(sizeof(addr.sun_path))) {
      THROW(HdfsNetworkException, "error computing UNIX domain socket path: %s",
            remoteAddr.c_str());
    }

    len = offsetof(struct sockaddr_un, sun_path) + strlen(addr.sun_path);

    do {
      rc = HdfsSystem::connect(sock, (struct sockaddr *)&addr, len);
    } while (rc < 0 && EINTR == errno && !CheckOperationCanceled());

    if (rc < 0) {
      THROW(HdfsNetworkConnectException, "Connect to \"%s:\" failed: %s", host,
            GetSystemErrorInfo(errno));
    }
  } catch (...) {
    close();
    throw;
  }
}

void DomainSocketImpl::connect(struct addrinfo *paddr, const char *host,
                               const char *port, int timeout) {
  assert(false && "not implemented");
  abort();
}

int32_t DomainSocketImpl::receiveFileDescriptors(int fds[], size_t nfds,
                                                 char *buffer, int32_t size) {
  assert(-1 != sock);
  ssize_t rc;
  struct iovec iov[1];
  struct msghdr msg;

  iov[0].iov_base = buffer;
  iov[0].iov_len = size;

  struct cmsghdr *cmsg;
  size_t auxSize = CMSG_SPACE(sizeof(int) * nfds);
  std::vector<char> aux(auxSize, 0); /* ancillary data buffer */

  memset(&msg, 0, sizeof(msg));
  msg.msg_iov = &iov[0];
  msg.msg_iovlen = 1;
  msg.msg_control = &aux[0];
  msg.msg_controllen = aux.size();
  cmsg = CMSG_FIRSTHDR(&msg);
  cmsg->cmsg_level = SOL_SOCKET;
  cmsg->cmsg_type = SCM_RIGHTS;
  cmsg->cmsg_len = CMSG_LEN(sizeof(int) * nfds);
  msg.msg_controllen = cmsg->cmsg_len;

  do {
    rc = HdfsSystem::recvmsg(sock, &msg, 0);
  } while (-1 == rc && EINTR == errno && !CheckOperationCanceled());

  if (-1 == rc) {
    THROW(HdfsNetworkException, "Read file descriptors failed from %s: %s",
          remoteAddr.c_str(), GetSystemErrorInfo(errno));
  }

  if (0 == rc) {
    THROW(HdfsEndOfStream,
          "Read file descriptors failed from %s: End of the stream",
          remoteAddr.c_str());
  }

  if (msg.msg_controllen != cmsg->cmsg_len) {
    THROW(HdfsEndOfStream, "Read file descriptors failed from %s.",
          remoteAddr.c_str());
  }

  int *fdptr = reinterpret_cast<int *>(CMSG_DATA(cmsg));

  for (size_t i = 0; i < nfds; ++i) {
    fds[i] = fdptr[i];
  }

  return rc;
}

}  // namespace Internal
}  // namespace Hdfs
