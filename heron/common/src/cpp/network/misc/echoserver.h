#ifndef __ECHOSERVER_H
#define __ECHOSERVER_H

#include "core/network/misc/tests.pb.h"
#include "core/network/public/event_loop_impl.h"
#include "core/network/public/server.h"
#include "core/network/public/networkoptions.h"
#include "network/network_error.h"

class EchoServer : public Server
{
 public:
  EchoServer(EventLoopImpl* ss, const NetworkOptions& options);
  ~EchoServer();

 protected:
  virtual void HandleNewConnection(Connection* newConnection);
  virtual void HandleConnectionClose(Connection* connection, NetworkErrorCode status);
  virtual void HandleEchoRequest(REQID id, Connection* connection,
                                 EchoServerRequest* request);

};

#endif
