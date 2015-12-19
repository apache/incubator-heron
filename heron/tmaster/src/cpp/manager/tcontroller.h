#ifndef __TCONTROLLER_H_
#define __TCONTROLLER_H_

namespace heron { namespace tmaster {

class TMaster;

class TController
{
 public:
  TController(EventLoop* eventLoop, const NetworkOptions& options,
              TMaster* tmaster);
  virtual ~TController();

  // Starts the controller
  sp_int32 Start();

 private:

  // Handlers for the requests
  // In all the below handlers, the incoming _request
  // parameter is now owned by the
  // TController class as is the norm with HeronServer.

  void HandleActivateRequest(IncomingHTTPRequest* request);
  void HandleActivateRequestDone(IncomingHTTPRequest* request,
                                 proto::system::StatusCode);
  void HandleDeActivateRequest(IncomingHTTPRequest* request);
  void HandleDeActivateRequestDone(IncomingHTTPRequest* request,
                                   proto::system::StatusCode);

  // We are a http server
  HTTPServer*   http_server_;

  // our tmaster
  TMaster*      tmaster_;
};

}} // end namespace

#endif
