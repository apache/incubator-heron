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

#include "basics/iputils.h"
#include <stdio.h>
#include <arpa/inet.h>
#include <ifaddrs.h>
#include <netdb.h>
#include <unistd.h>
#include <string>
#include "glog/logging.h"
#include "basics/sptypes.h"
#include "basics/sprcodes.h"

bool IpUtils::checkIPAddress(const std::string& ip_addr, const IPAddress_Set& aset) {
  auto it = aset.find(ip_addr);
  return it != aset.end() ? true : false;
}

std::string IpUtils::getHostName() {
  char hostname[BUFSIZ];
  struct addrinfo hints, *info;
  int gai_result;

  ::gethostname(hostname, sizeof(hostname));

  memset(&hints, 0, sizeof hints);
  hints.ai_family = AF_UNSPEC; /*either IPV4 or IPV6*/
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_flags = AI_CANONNAME;

  if ((gai_result = ::getaddrinfo(hostname, nullptr, &hints, &info)) != 0) {
    LOG(WARNING) << "getaddrinfo returned non zero result " << gai_strerror(gai_result);
    return std::string(hostname);
  }

  if (!info) {
    return std::string(hostname);
  }

  std::string cannonical_name = info->ai_canonname;
  freeaddrinfo(info);
  return cannonical_name;
}

sp_int32 IpUtils::getIPAddressHost(IPAddress_Set& aset) {
  if (!getIPAddress(aset)) return SP_NOTOK;

  aset.insert(getHostName());
  return SP_OK;
}

sp_int32 IpUtils::getIPAddress(IPAddress_Set& aset) {
  struct ifaddrs* ifAddrStruct = nullptr;
  struct ifaddrs* ifa = nullptr;

  aset.clear();
  getifaddrs(&ifAddrStruct);

  for (ifa = ifAddrStruct; ifa != nullptr; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr->sa_family == AF_INET) {
      struct sockaddr_in* addr_in = (struct sockaddr_in*)ifa->ifa_addr;
      char addressBuffer[INET_ADDRSTRLEN];
      inet_ntop(AF_INET, &addr_in->sin_addr, addressBuffer, INET_ADDRSTRLEN);

      // We don't need the loopback address
      if (strcmp("lo", ifa->ifa_name)) aset.insert(std::string(addressBuffer));
    }
  }

  if (ifAddrStruct != nullptr) freeifaddrs(ifAddrStruct);
  return aset.empty() ? SP_NOTOK : SP_OK;
}

sp_int32 IpUtils::getIPV6Address(IPAddress_Set& aset) {
  struct ifaddrs* ifAddrStruct = nullptr;
  struct ifaddrs* ifa = nullptr;

  aset.clear();
  getifaddrs(&ifAddrStruct);

  for (ifa = ifAddrStruct; ifa != nullptr; ifa = ifa->ifa_next) {
    if (ifa->ifa_addr->sa_family == AF_INET6) {
      struct sockaddr_in6* addr_in = (struct sockaddr_in6*)ifa->ifa_addr;
      char addressBuffer[INET6_ADDRSTRLEN];
      inet_ntop(AF_INET6, &addr_in->sin6_addr, addressBuffer, INET6_ADDRSTRLEN);

      // We don't need the loopback address
      if (strcmp("lo", ifa->ifa_name)) aset.insert(std::string(addressBuffer));
    }
  }

  if (ifAddrStruct != nullptr) freeifaddrs(ifAddrStruct);
  return aset.empty() ? SP_NOTOK : SP_OK;
}

sp_int32 IpUtils::getAddressInfo(struct sockaddr_in& t, const char* host, int family, int type) {
  struct addrinfo hints, *res;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = family;
  hints.ai_socktype = type;
  int error = getaddrinfo(host, nullptr, &hints, &res);
  if (error == 0) {
    t = *((struct sockaddr_in*)res->ai_addr);
    freeaddrinfo(res);
  }
  return error;
}
