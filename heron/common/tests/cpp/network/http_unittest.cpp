#include <cstddef>
#include <iostream>
#include <thread>
#include <vector>

#include "gtest/gtest.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "basics/modinit.h"
#include "errors/modinit.h"
#include "threads/modinit.h"
#include "network/modinit.h"

#include "network/host_unittest.h"

static const sp_uint32 SERVER_PORT = 60000;

extern void
start_http_client(
  sp_uint32 _port,
  sp_uint64 _requests,
  sp_uint32 _nkeys
);

extern void
start_http_server(sp_uint32 _port, sp_uint32 _nkeys);

void
TerminateRequestDone(HTTPClient* client, IncomingHTTPResponse* response)
{
  delete response;
  client->getEventLoop()->loopExit();
}

void
TerminateServer(sp_uint32 port)
{
  EventLoopImpl ss;
  AsyncDNS dns(&ss);
  HTTPClient client(&ss, &dns);

  HTTPKeyValuePairs kvs;

  OutgoingHTTPRequest* request =
    new OutgoingHTTPRequest(LOCALHOST, port,
      "/terminate", BaseHTTPRequest::GET, kvs);

  auto cb = [&client] (IncomingHTTPResponse* response) {
    TerminateRequestDone(&client, response);
  };

  auto ret = client.SendRequest(request, cb);
  if (ret != SP_OK) {
    GTEST_FAIL();
  }

  ss.loop();
}

void
StartTest(sp_uint32 nclients, sp_uint64 requests, sp_uint32 nkeys)
{
  // start the server thread
  std::thread sthread(start_http_server, SERVER_PORT, nkeys);

  ::usleep(1000);

  // start the client threads
  std::vector<std::thread> cthreads;
  for (sp_uint32 i = 0 ; i < nclients ; i++)
  {
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
}

TEST(NetworkTest, test_http)
{
  StartTest(1, 1000, 1);
}

int
main(int argc, char **argv)
{
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
