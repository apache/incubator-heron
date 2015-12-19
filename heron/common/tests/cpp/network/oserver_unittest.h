#ifndef _ORDER_SERVER_H
#define _ORDER_SERVER_H

#include "network/network_error.h"

class OrderServer : public Server
{
 public:
  OrderServer
  (
    EventLoopImpl*         ss,
    const NetworkOptions& options
  );

  ~OrderServer();

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
  HandleOrderMessage(Connection* connection, OrderMessage* message);

  // handle the terminate message
  virtual void
  HandleTerminateMessage(Connection* connection, TerminateMessage* message);

 private:

  void Terminate();

  class msgid
  {
   public:
    msgid() : ids_(0), idr_(0) { }
    ~msgid() { }

    sp_uint64 incr_ids() {
      sp_uint64 t = ids_++ ; return t;
    }

    sp_uint64 incr_idr() {
      sp_uint64 t = idr_++ ; return t;
    }

   private:
    sp_uint64 ids_;
    sp_uint64 idr_;
  } ;

  std::map<Connection*, msgid*>       clients_;

  sp_uint64                           nrecv_;
  sp_uint64                           nsent_;
};

#endif
