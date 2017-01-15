#ifndef __SAMPLEHTTPSERVER_H
#define __SAMPLEHTTPSERVER_H

#include "core/network/public/event_loop_impl.h"
#include "core/network/public/httpserver.h"

class SampleHTTPServer
{
 public:
  SampleHTTPServer(EventLoopImpl* ss, ServerOptions* options);
  ~SampleHTTPServer();

  void HandleMetaRequest(IncomingHTTPRequest* _request);
  void HandleGenericRequest(IncomingHTTPRequest* _request);

 private:
  HTTPServer* server_;
};

#endif
