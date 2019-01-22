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
#include "core/network/misc/samplehttpserver.h"

int main(int argc, char* argv[])
{
  Init("SampleHTTPServer", argc, argv);

  if (argc < 2) {
    cout << "Usage " << argv[0] << " <port>\n";
    exit(1);
  }

  EventLoopImpl ss;
  ServerOptions options;
  options.set_host("127.0.0.1");
  options.set_port(atoi(argv[1]));
  options.set_max_packet_size(0);
  SampleHTTPServer http_server(&ss, &options);
  ss.loop();
  return 0;
}
