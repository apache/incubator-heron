#ifndef __TEST_CLIENT_H
#define __TEST_CLIENT_H

#include <functional>
#include "network/network_error.h"

class TestClient : public Client
{
 public:
  TestClient
  (
    EventLoopImpl*         eventLoop,
    const NetworkOptions& _options,
    sp_uint64             _ntotal
  );

  ~TestClient();

 protected:

  void Retry() { Start(); }

  // Handle incoming connections
  virtual void HandleConnect(NetworkErrorCode _status);

  // Handle connection close
  virtual void HandleClose(NetworkErrorCode _status);

 private:

  // Handle incoming message
  void HandleTestMessage(TestMessage* _message);

  void SendMessages();
  void CreateAndSendMessage();

  VCallback<>          retry_cb_;

  time_t               start_time_;

  sp_uint64            nsent_;
  sp_uint64            nrecv_;
  sp_uint64            ntotal_;
};

#endif
