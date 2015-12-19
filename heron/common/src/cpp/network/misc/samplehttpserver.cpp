#include <iostream>

#include "core/network/misc/samplehttpserver.h"
#include "core/common/public/common.h"
#include "core/errors/public/errors.h"
#include "core/threads/public/threads.h"
#include "core/network/public/network.h"

SampleHTTPServer::SampleHTTPServer(EventLoopImpl* eventLoop, ServerOptions* _options)
{
  server_ = new HTTPServer(eventLoop, _options);
  server_->InstallCallBack("/meta", CreatePermanentCallback(this, &SampleHTTPServer::HandleMetaRequest));
  server_->InstallGenericCallBack(CreatePermanentCallback(this, &SampleHTTPServer::HandleGenericRequest));
  server_->Start();
}

SampleHTTPServer::~SampleHTTPServer()
{
  delete server_;
}

void SampleHTTPServer::HandleMetaRequest(IncomingHTTPRequest* _request)
{
  cout << "Got a new meta request\n";
  if (_request->type() != BaseHTTPRequest::GET) {
    // We only accept get requests
    server_->SendErrorReply(_request, 400);
    return;
  }
  const HTTPKeyValuePairs& keyvalues = _request->keyvalues();
  cout << "The keyvalue count is " << keyvalues.size() << "\n";
  for (size_t i = 0; i < keyvalues.size(); ++i) {
    cout << "Key : " << keyvalues[i].first << " Value: "
         << keyvalues[i].second << "\n";
  }
  OutgoingHTTPResponse* response = new OutgoingHTTPResponse(_request);
  response->AddResponse("This is response for Meta Object\r\n");
  server_->SendReply(_request, 200, response);
}

void SampleHTTPServer::HandleGenericRequest(IncomingHTTPRequest* _request)
{
  cout << "Got a generic request for uri " << _request->GetQuery() << "\n";
  const HTTPKeyValuePairs& keyvalues = _request->keyvalues();
  cout << "The keyvalue count is " << keyvalues.size() << "\n";
  for (size_t i = 0; i < keyvalues.size(); ++i) {
    cout << "Key : " << keyvalues[i].first << " Value: "
         << keyvalues[i].second << "\n";
  }
  server_->SendErrorReply(_request, 404);
}
