#include "gtest/gtest.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "network/host_unittest.h"

static sp_uint64 ntotal = 0;
static sp_uint64 nreqs = 0;
static sp_uint64 nresps = 0;
static sp_uint32 port = -1;
static sp_uint32 nkeys = 0;

void
RequestDone(HTTPClient* _client, IncomingHTTPResponse* _response);

void
SendRequest(HTTPClient* client)
{
  HTTPKeyValuePairs kvs;

  for (sp_uint32 i = 0 ; i < nkeys ; i++)
  {
    std::ostringstream key, value;
    key << "key" << i;
    value << "value" << i;
    kvs.push_back(make_pair(key.str(),value.str()));
  }

  OutgoingHTTPRequest* request =
    new OutgoingHTTPRequest(LOCALHOST, port,
      "/meta", BaseHTTPRequest::GET, kvs);

  auto cb = [client] (IncomingHTTPResponse* response) {
    RequestDone(client, response);
  };

  if (client->SendRequest(request, std::move(cb)) != SP_OK)
  {
    GTEST_FAIL();
  }

  nreqs++;
}

void
RequestDone(HTTPClient* _client, IncomingHTTPResponse* _response)
{
  nresps++;
  EXPECT_EQ(200, _response->response_code());

  delete _response;

  if (nreqs < ntotal)
  {
    SendRequest(_client); return;
  }

  _client->getEventLoop()->loopExit();
}

void
start_http_client(sp_uint32 _port, sp_uint64 _requests, sp_uint32 _nkeys)
{
  port = _port;
  ntotal = _requests;
  nkeys = _nkeys;

  EventLoopImpl ss;
  AsyncDNS dns(&ss);
  HTTPClient client(&ss, &dns);
  SendRequest(&client);
  ss.loop();
}
