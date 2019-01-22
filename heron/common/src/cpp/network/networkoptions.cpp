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

#include "network/networkoptions.h"
#include <arpa/inet.h>
#include <string>
#include "basics/spconsts.h"

// This is the default high water mark on the num of bytes that can be left outstanding on
// a connection
const sp_int64 systemHWMOutstandingBytes = 100_MB;
// This is the default low water mark on the num of bytes that can be left outstanding on
// a connection
const sp_int64 systemLWMOutstandingBytes = 50_MB;

NetworkOptions::NetworkOptions() {
  host_ = "127.0.0.1";
  port_ = 8080;
  max_packet_size_ = 1_KB;
  socket_family_ = PF_INET;
  sin_path_ = "";
  high_watermark_ = systemHWMOutstandingBytes;
  low_watermark_ = systemLWMOutstandingBytes;
}

NetworkOptions::NetworkOptions(const NetworkOptions& _copyFrom) {
  host_ = _copyFrom.get_host();
  port_ = _copyFrom.get_port();
  max_packet_size_ = _copyFrom.get_max_packet_size();
  socket_family_ = _copyFrom.get_socket_family();
  high_watermark_ = _copyFrom.get_high_watermark();
  low_watermark_ = _copyFrom.get_low_watermark();
  sin_path_ = _copyFrom.get_sin_path();
}

NetworkOptions::~NetworkOptions() {}

void NetworkOptions::set_host(const std::string& _host) { host_ = _host; }

std::string NetworkOptions::get_host() const { return host_; }

void NetworkOptions::set_port(sp_int32 _port) { port_ = _port; }

sp_int32 NetworkOptions::get_port() const { return port_; }

void NetworkOptions::set_max_packet_size(sp_uint32 _max_packet_size) {
  max_packet_size_ = _max_packet_size;
}

sp_uint32 NetworkOptions::get_max_packet_size() const { return max_packet_size_; }

void NetworkOptions::set_high_watermark(sp_int64 _high_watermark) {
  high_watermark_ = _high_watermark;
}

sp_int64 NetworkOptions::get_high_watermark() const { return high_watermark_; }

void NetworkOptions::set_low_watermark(sp_int64 _low_watermark) {
  low_watermark_ = _low_watermark;
}

sp_int64 NetworkOptions::get_low_watermark() const { return low_watermark_; }

void NetworkOptions::set_socket_family(sp_int32 _socket_family) { socket_family_ = _socket_family; }

sp_int32 NetworkOptions::get_socket_family() const { return socket_family_; }

sp_int32 NetworkOptions::get_sin_family() const {
  if (socket_family_ == PF_INET)
    return AF_INET;
  else
    return AF_UNIX;
}

void NetworkOptions::set_sin_path(const std::string& _sin_path) { sin_path_ = _sin_path; }

const std::string& NetworkOptions::get_sin_path() const { return sin_path_; }
