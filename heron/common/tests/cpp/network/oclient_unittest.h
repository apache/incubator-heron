#ifndef __ORDER_CLIENT_H
#define __ORDER_CLIENT_H

#include "network/network_error.h"

class OrderClient : public Client
{
 public:
  OrderClient
  (
    EventLoopImpl*         eventLoop,
    const NetworkOptions& _options,
    sp_uint64             _ntotal
  );

  ~OrderClient() { }

 protected:

  void Retry() { Start(); }

  // Handle incoming connections
  virtual void HandleConnect(NetworkErrorCode _status);

  // Handle connection close
  virtual void HandleClose(NetworkErrorCode _status);

 private:

  // Handle incoming message
  void HandleOrderMessage(OrderMessage* _message);

  void SendMessages();
  void CreateAndSendMessage();

  VCallback<>          retry_cb_;

  time_t               start_time_;

  sp_uint64            msgids_;
  sp_uint64            msgidr_;

  sp_uint64            nsent_;
  sp_uint64            nrecv_;
  sp_uint64            ntotal_;
};

#endif
