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

#include <thread>
#include <vector>
#include <chrono>
#include "network/unittests.pb.h"
#include "network/host_unittest.h"
#include "network/client_unittest.h"
#include "network/server_unittest.h"
#include "gtest/gtest.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "threads/spcountdownlatch.h"
#include "network/network.h"
#include "basics/modinit.h"
#include "errors/modinit.h"
#include "threads/modinit.h"
#include "network/modinit.h"

class Terminate : public Client {
 public:
  Terminate(std::shared_ptr<EventLoopImpl> eventLoop, const NetworkOptions& _options)
      : Client(eventLoop, _options) {
    // Setup the call back function to be invoked when retrying
    retry_cb_ = [this]() { this->Retry(); };
  }

  ~Terminate() {}

 protected:
  void Retry() { Start(); }

  virtual void HandleConnect(NetworkErrorCode _status) {
    if (_status == OK) {
      TerminateMessage message;
      SendMessage(message);
      return;
    }

    AddTimer(retry_cb_, 1000);
  }

  virtual void HandleClose(NetworkErrorCode) {
    Stop();
    getEventLoop()->loopExit();
  }

 private:
  VCallback<> retry_cb_;
};

class RateLimitTestServer : public TestServer {
 public:
  RateLimitTestServer(std::shared_ptr<EventLoopImpl> ss, const NetworkOptions& options,
          sp_int64 rate)
      : TestServer(ss, options) {
    rate_ = rate;
  }

  void HandleNewConnection(Connection* _conn) {
    TestServer::HandleNewConnection(_conn);
    _conn->setRateLimit(rate_, rate_);
  }
 private:
  sp_int64 rate_;
};

static RateLimitTestServer* server_;

void start_server(sp_uint32* port, CountDownLatch* latch, sp_int64 rate) {
  NetworkOptions options;
  options.set_host(LOCALHOST);
  options.set_port(*port);
  options.set_max_packet_size(1024 * 1024);
  options.set_socket_family(PF_INET);

  auto ss = std::make_shared<EventLoopImpl>();
  server_ = new RateLimitTestServer(ss, options, rate);
  EXPECT_EQ(0, server_->get_serveroptions().get_port());
  if (server_->Start() != 0) GTEST_FAIL();
  *port = server_->get_serveroptions().get_port();
  latch->countDown();
  ss->loop();
}

void start_client(sp_uint32 port, sp_uint64 requests) {
  NetworkOptions options;
  options.set_host(LOCALHOST);
  options.set_port(port);
  options.set_max_packet_size(1024 * 1024);
  options.set_socket_family(PF_INET);

  auto ss = std::make_shared<EventLoopImpl>();
  TestClient client(ss, options, requests);
  client.Start();
  ss->loop();
}

void terminate_server(sp_uint32 port) {
  NetworkOptions options;
  options.set_host(LOCALHOST);
  options.set_port(port);
  options.set_max_packet_size(1024 * 1024);
  options.set_socket_family(PF_INET);

  auto ss = std::make_shared<EventLoopImpl>();
  Terminate ts(ss, options);
  ts.Start();
  ss->loop();
}

void start_test(sp_int32 nclients,
                sp_uint64 requests,
                sp_int64 rate,
                sp_int64 expected_threshold) {
  sp_uint32 server_port = 0;
  CountDownLatch* latch = new CountDownLatch(1);

  // start the server thread
  std::thread sthread(start_server, &server_port, latch, rate);
  latch->wait();
  std::cout << "server port " << server_port << std::endl;

  auto start = std::chrono::high_resolution_clock::now();

  // start the client threads
  std::vector<std::thread> cthreads;
  for (sp_int32 i = 0; i < nclients; i++) {
    cthreads.push_back(std::thread(start_client, server_port, requests));
  }

  // wait for the client threads to terminate
  for (auto& thread : cthreads) {
    thread.join();
  }

  auto stop = std::chrono::high_resolution_clock::now();

  // now send a terminate message to server
  terminate_server(server_port);
  sthread.join();

  ASSERT_TRUE(server_->sent_pkts() == server_->recv_pkts());
  ASSERT_TRUE(server_->sent_pkts() == nclients * requests);

  auto delay = std::chrono::duration_cast<std::chrono::milliseconds>(stop - start).count();
  std::cout << nclients << " client(s) exchanged a total of " << requests << " in "
            <<  delay << " ms."
            << std::endl;
  // because of rate limiting, the process time should be longer than the expected threshold
  ASSERT_TRUE(delay > expected_threshold);

  delete server_;
  delete latch;
}

// Without rate limiting, the process last about 10~20ms on my laptop. The process time relies
// on the hardware and os the tests are running on. However with rate limiting, the process time
// should be more stable/predictable
TEST(NetworkTest, test_rate_limit_1) { start_test(1, 100, 50000, 1500); }

TEST(NetworkTest, test_rate_limit_2) { start_test(1, 100, 75000, 700); }

TEST(NetworkTest, test_rate_limit_3) { start_test(1, 100, 100000, 500); }

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
