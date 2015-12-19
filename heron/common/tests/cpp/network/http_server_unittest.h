#ifndef __TEST_HTTP_SERVER_H_
#define __TEST_HTTP_SERVER_H_

class TestHttpServer
{
 public:
  // Constructor
  TestHttpServer(EventLoopImpl* ss, NetworkOptions& options);

  // Destructor
  ~TestHttpServer();

  // Handle for a URL meta request
  void HandleMetaRequest(IncomingHTTPRequest* _request);

  // Handler for any type of generic request
  void HandleGenericRequest(IncomingHTTPRequest* _request);

  // Handle for a URL terminate request
  void HandleTerminateRequest(IncomingHTTPRequest* _request);

 private:

  HTTPServer*       server_;
};

#endif
