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

#if !defined(HERON_SOCK_UTILS_H_)
#define HERON_SOCK_UTILS_H_

#include "basics/sptypes.h"

class SockUtils {
 public:
  //! Set socket non blocking
  static sp_int32 setNonBlocking(sp_int32 fd);

  //! Get and set socket send buffer
  static sp_int32 getSendBufferSize(sp_int32 fd, sp_int32& size);
  static sp_int32 setSendBufferSize(sp_int32 fd, sp_int32 size);

  //! Get and set socket recv buffer
  static sp_int32 getRecvBufferSize(sp_int32 fd, sp_int32& size);
  static sp_int32 setRecvBufferSize(sp_int32 fd, sp_int32 size);

  //! Set socket keep alive
  static sp_int32 setKeepAlive(sp_int32 fd);

  //! Set socket reuse addr
  static sp_int32 setReuseAddress(sp_int32 fd);

  //! Set socket keep idle time
  static sp_int32 setKeepIdleTime(sp_int32 fd, sp_int32 time);

  //! Set socket keep count
  static sp_int32 setKeepIdleCount(sp_int32 fd, sp_int32 count);

  //! Set socket keep interval
  static sp_int32 setKeepIdleInterval(sp_int32 fd, sp_int32 interval);

  //! Set socket keep interval
  static sp_int32 setKeepIdleParams(sp_int32 fd, sp_int32 time, sp_int32 count, sp_int32 interval);

  //! disable nagle's algo
  static sp_int32 setTcpNoDelay(sp_int32 fd);

  //! Set reasonable default values for a socket
  static sp_int32 setSocketDefaults(sp_int32 fd);
};

#endif
