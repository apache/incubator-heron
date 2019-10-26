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

#ifndef NETWORKOPTIONS_H_
#define NETWORKOPTIONS_H_

#include <string>
#include "basics/basics.h"
/*
 * NetworkOptions class definition.
 * One can specify things like which host/port to bind on.
 * whats the maximum packet length allowed and so on.
 */
class NetworkOptions {
 public:
  NetworkOptions();
  NetworkOptions(const NetworkOptions& _copyFrom);
  ~NetworkOptions();

  // set functions for inet address family
  void set_host(const sp_string& host);
  void set_port(sp_int32 port);

  // get/set for packet size
  void set_max_packet_size(sp_uint32 max_packet_size);

  // set the socket family
  void set_socket_family(sp_int32 socket_family);

  // set high water mark for back pressure
  void set_high_watermark(sp_int64 _high_watermark);

  // set low water mark for back pressure
  void set_low_watermark(sp_int64 _low_watermark);

  // set the sin path
  // applicable only for unix sockets
  void set_sin_path(const std::string& socket_path);

  // get functions for inet address family
  sp_string get_host() const;
  sp_int32 get_port() const;

  // get functions for unix address family
  sp_uint32 get_max_packet_size() const;

  // get high water mark for back pressure
  sp_int64 get_high_watermark() const;

  // get low water mark for back pressure
  sp_int64 get_low_watermark() const;

  // get the family type
  sp_int32 get_socket_family() const;

  // get the sin family
  sp_int32 get_sin_family() const;

  // get the unix path
  const std::string& get_sin_path() const;

 private:
  // The host that we shd bind on
  sp_string host_;

  // The port that we shd bind to.
  sp_int32 port_;

  // Whats the maximum packet size to expect
  sp_uint32 max_packet_size_;

  // Whats the socket family
  sp_int32 socket_family_;

  // Whats the socket path
  // applicable only for UNIX sockets
  std::string sin_path_;

  sp_int64 high_watermark_;
  sp_int64 low_watermark_;
};

#endif  // NETWORKOPTIONS_H_
