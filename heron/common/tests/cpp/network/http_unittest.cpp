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

#include <unistd.h>
#include <cstddef>
#include <iostream>
#include <thread>
#include <vector>

#include "network/host_unittest.h"
#include "gtest/gtest.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "basics/modinit.h"
#include "errors/modinit.h"
#include "threads/modinit.h"
#include "network/modinit.h"


static const sp_uint32 SERVER_PORT = 60000;

extern void start_http_client(sp_uint32 _port, sp_uint64 _requests, sp_uint32 _nkeys);

extern void start_http_server(sp_uint32 _port, sp_uint32 _nkeys, int fd);

void TerminateRequestDone(HTTPClient* client, IncomingHTTPResponse* response) {
  delete response;
  client->getEventLoop()->loopExit();
}

void TerminateServer(sp_uint32 port) {
  auto ss = std::make_shared<EventLoopImpl>();
  AsyncDNS dns(ss);
  HTTPClient client(ss, &dns);

  HTTPKeyValuePairs kvs;

  OutgoingHTTPRequest* request =
      new OutgoingHTTPRequest(LOCALHOST, port, "/terminate", BaseHTTPRequest::GET, kvs);

  auto cb = [&client](IncomingHTTPResponse* response) { TerminateRequestDone(&client, response); };

  auto ret = client.SendRequest(request, cb);
  if (ret != SP_OK) {
    GTEST_FAIL();
  }

  ss->loop();
}

void StartTest(sp_uint32 nclients, sp_uint64 requests, sp_uint32 nkeys) {
  int fds[2];
  int recv;
  if (::pipe(fds) < 0) {
    std::cerr << "Unable to open pipe" << std::endl;
    GTEST_FAIL();
  }

  // start the server thread
  std::thread sthread(start_http_server, SERVER_PORT, nkeys, fds[1]);

  // block clients from reading from server using pipe
  read(fds[0], &recv, sizeof(int));

  // start the client threads
  std::vector<std::thread> cthreads;
  for (sp_uint32 i = 0; i < nclients; i++) {
    cthreads.push_back(std::thread(start_http_client, SERVER_PORT, requests, nkeys));
  }

  // wait for the client threads to terminate
  for (auto& thread : cthreads) {
    thread.join();
  }

  // now collect the stats from sthread
  std::thread tthread(TerminateServer, SERVER_PORT);
  tthread.join();

  // wait until the server is done
  sthread.join();

  close(fds[0]);
  close(fds[1]);
}

TEST(NetworkTest, test_http) { StartTest(1, 1000, 1); }

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
