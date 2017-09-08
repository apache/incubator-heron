#ifndef __ECHOCLIENT_H
#define __ECHOCLIENT_H

#include "core/network/misc/tests.pb.h"
#include "core/network/public/event_loop_impl.h"
#include "core/network/public/client.h"
#include "core/network/public/networkoptions.h"
#include "network/network_error.h"
#include "core/common/public/sptypes.h"

class EchoClient : public Client
{
 public:
  EchoClient(EventLoopImpl* ss, const NetworkOptions& options, bool _perf);
  ~EchoClient();

 protected:
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

 private:
  void HandleEchoResponse(void*, EchoServerResponse* response,
                          NetworkErrorCode status);
  void CreateAndSendRequest();
  sp_int32 nrequests_;
  bool perf_;
};

#endif
