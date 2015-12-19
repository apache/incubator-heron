#ifndef __TESTSERVER_H
#define __TESTSERVER_H

#include "network/network_error.h"

class TestServer : public Server
{
 public:
  TestServer
  (
    EventLoopImpl*         ss,
    const NetworkOptions& options
  );

  ~TestServer();

  // total packets recvd
  sp_uint64 recv_pkts() { return nrecv_; }

  // total packets sent
  sp_uint64 sent_pkts() { return nsent_; }

 protected:

  // handle an incoming connection from server
  virtual void
  HandleNewConnection(Connection* newConnection);

  // handle a connection close
  virtual void
  HandleConnectionClose(Connection* connection, NetworkErrorCode status);

  // handle the test message
  virtual void
  HandleTestMessage(Connection* connection, TestMessage* message);

  // handle the terminate message
  virtual void
  HandleTerminateMessage(Connection* connection, TerminateMessage* message);

 private:

  void Terminate();

  std::set<Connection*>               clients_;
  std::vector<Connection*>            vclients_;

  sp_uint64                           nrecv_;
  sp_uint64                           nsent_;
};

#endif
