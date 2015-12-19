#include <iostream>

#include "core/common/public/common.h"
#include "core/errors/public/errors.h"
#include "core/threads/public/threads.h"
#include "core/network/public/network.h"

lx_int32 port = -1;

void SendRequest(HTTPClient* _client);

void RequestDone(HTTPClient* _client, IncomingHTTPResponse* _response)
{
  if (_response->response_code() != 200) {
    cout << "Error response quitting\n";
  } else {
    cout << "The body is " << _response->body() << "\n";
    SendRequest(_client);
  }
  delete _response;
}


void SendRequest(HTTPClient* _client)
{
  HTTPKeyValuePairs kvs;
  kvs.push_back(make_pair("key", "value"));
  OutgoingHTTPRequest* request = new OutgoingHTTPRequest("127.0.0.1", port,
                                                         "/meta",
                                                         BaseHTTPRequest::GET, kvs);
  if (_client->SendRequest(request, CreateCallback(&RequestDone, _client)) != LX_OK) {
    cout << "Unable to send the request\n";
  }
}

int main(int argc, char* argv[])
{
  Init("SampleHTTPClient", argc, argv);

  if (argc < 2) {
    cout << "Usage " << argv[0] << " <port>\n";
    exit(1);
  }
  port = atoi(argv[1]);

  EventLoopImpl ss;
  AsyncDNS dns(&ss);
  HTTPClient client(&ss, &dns);
  SendRequest(&client);
  ss.loop();
  return 0;
}
