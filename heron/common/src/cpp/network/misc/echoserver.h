#ifndef __ECHOSERVER_H
#define __ECHOSERVER_H

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "proto/messages.h"

class EchoServer : public Server
{
 public:
  EchoServer(EventLoopImpl* ss, const NetworkOptions& options);
  ~EchoServer();

 protected:
  virtual void HandleNewConnection(Connection* newConnection);
  virtual void HandleConnectionClose(Connection* connection, NetworkErrorCode status);
  virtual void HandleEchoMessage(Connection* conn, heron::proto::system::RootId* message);
};

#endif
