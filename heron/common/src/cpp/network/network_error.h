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

#ifndef _CORE_SRC_CPP_NEWTORK_NETWORK_ERROR_H_
#define _CORE_SRC_CPP_NEWTORK_NETWORK_ERROR_H_

#include <ostream>
#include <string>

enum NetworkErrorCode {
  OK = 0,
  CONNECT_ERROR,
  READ_ERROR,
  WRITE_ERROR,
  CLOSE_ERROR,
  INVALID_CONNECTION,
  DUPLICATE_START,
  INVALID_PACKET,
  DUPLICATE_PACKET_ID,
  TIMEOUT
};

static inline std::ostream& operator<<(std::ostream& os, const NetworkErrorCode code) {
  std::string strs[] = { "OK", "Connection error", "Read error", "Write error",
                         "Close error", "Invalid connection", "Duplicate start",
                         "Invalid packet", "Duplicate packet ID", "Timeout" };
  os << strs[code];
  return os;
}

#endif  // _CORE_SRC_CPP_NEWTORK_NETWORK_ERROR_H_
