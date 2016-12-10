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
#ifndef _HDFS_LIBHDFS3_NETWORK_DOMAINSOCKET_H_
#define _HDFS_LIBHDFS3_NETWORK_DOMAINSOCKET_H_

#include "common/platform.h"
#include "network/TcpSocket.h"

namespace Hdfs {
namespace Internal {

/**
 * A Domain socket client
 */
class DomainSocketImpl : public TcpSocketImpl {
 public:
  /**
   * Construct a Socket object.
   * @throw nothrow
   */
  DomainSocketImpl();

  /**
   * Destroy a DomainSocketImpl instance.
   */
  ~DomainSocketImpl();

  /**
   * Connection to a domain socket server.
   * @param host The host of server.
   * @param port The port of server.
   * @param timeout The timeout interval of this read operation, negative
   * means infinite.
   * @throw HdfsNetworkException
   * @throw HdfsTimeout
   */
  void connect(const char *host, int port, int timeout);

  /**
   * Connection to a domain socket server.
   * @param host The host of server.
   * @param port The port of server.
   * @param timeout The timeout interval of this read operation, negative
   * means infinite.
   * @throw HdfsNetworkException
   * @throw HdfsTimeout
   */
  void connect(const char *host, const char *port, int timeout);

  /**
   * Connection to a domain socket server.
   * @param paddr The address of server.
   * @param host The host of server used in error message.
   * @param port The port of server used in error message.
   * @param timeout The timeout interval of this read operation, negative
   * means infinite.
   * @throw HdfsNetworkException
   * @throw HdfsTimeout
   */
  void connect(struct addrinfo *paddr, const char *host, const char *port,
               int timeout);

  /**
   * Read file descriptors from domain socket.
   *
   * @param fds buffer to hold the received file descriptors.
   * @param nfds number of file descriptors needs to receive.
   * @param buffer the buffer to receive data
   * @size  buffer size to receive data
   *
   * @return return the number of received bytes.
   */
  int32_t receiveFileDescriptors(int fds[], size_t nfds, char *buffer,
                                 int32_t size);
};

}  // namespace Internal
}  // namespace Hdfs

#endif /* _HDFS_LIBHDFS3_NETWORK_DOMAINSOCKET_H_ */
