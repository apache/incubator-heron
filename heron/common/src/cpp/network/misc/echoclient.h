#ifndef __ECHOCLIENT_H
#define __ECHOCLIENT_H

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "proto/messages.h"

class EchoClient : public Client
{
 public:
  EchoClient(EventLoopImpl* ss, const NetworkOptions& options);
  ~EchoClient();

 protected:
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

 private:
  void HandleEchoMessage(heron::proto::system::RootId* message);
  void CreateAndSendMessage();
  sp_int32 nrequests_;
};

#endif
