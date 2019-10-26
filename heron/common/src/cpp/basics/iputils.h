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

////////////////////////////////////////////////////////////////////////////////
//
// Utility functions for IPaddress
//
///////////////////////////////////////////////////////////////////////////////

#if !defined(HERON_IP_UTILS_H_)
#define HERON_IP_UTILS_H_

#include <set>
#include <string>
#include "basics/sptypes.h"

typedef std::set<std::string> IPAddress_Set;

class IpUtils {
 public:
  // get the ip address and host name
  static sp_int32 getIPAddressHost(IPAddress_Set& aset);

  // get the hostname
  static std::string getHostName();

  // get the ip v4 address of the host
  static sp_int32 getIPAddress(IPAddress_Set& aset);

  // get the ip v6 address of the host
  static sp_int32 getIPV6Address(IPAddress_Set& aset);

  // check the ip address
  static bool checkIPAddress(const std::string& ip_address, const IPAddress_Set& aset);

  // get the ip address associated with host
  static sp_int32 getAddressInfo(struct sockaddr_in& t, const char* host, int family, int type);
};

#endif  // HERON_IP_UTILS_H_
