/*
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

#include <iostream>

#include "core/common/public/common.h"
#include "core/errors/public/errors.h"
#include "core/threads/public/threads.h"
#include "core/network/public/network.h"

#include "core/network/misc/tests.pb.h"
#include "core/network/misc/echoserver.h"

int main(int argc, char* argv[])
{
  if (argc < 4) {
    std::cout << "Usage: " << argv[0] << " <host> <port> <local/remote>" << std::endl;
    exit(1);
  }
  NetworkOptions options;
  options.set_host(argv[1]);
  options.set_port(atoi(argv[2]));
  options.set_max_packet_size(1024);
  if (strcmp(argv[3], "local") == 0) {
    options.set_socket_family(PF_UNIX);
    options.set_sin_path("/tmp/__echoserver__");
  } else {
    options.set_socket_family(PF_INET);
  }

  EventLoopImpl ss;
  EchoServer echo_server(&ss, options);
  if (echo_server.Start() != 0) {
    // What the hell happened
    std::cout << "Server failed to start\n";
    return 1;
  }
  ss.loop();
  return 0;
}
