#include <iostream>
#include <chrono>

#include "gtest/gtest.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "network/unittests.pb.h"
#include "network/oserver_unittest.h"

OrderServer::OrderServer
 (
   EventLoopImpl*         eventLoop,
   const NetworkOptions& _options
 ) : Server(eventLoop, _options)
{
  InstallMessageHandler(&OrderServer::HandleOrderMessage);
  InstallMessageHandler(&OrderServer::HandleTerminateMessage);
  nrecv_ = nsent_ = 0;
}

OrderServer::~OrderServer()
{
}

void
OrderServer::HandleNewConnection(Connection* _conn)
{
  if (clients_.find(_conn) != clients_.end())
    return;

  clients_[_conn] = new msgid;
}

void
OrderServer::HandleConnectionClose
 (
   Connection*      _conn,
   NetworkErrorCode _status __attribute__((unused))
 )
{
  if (clients_.find(_conn) == clients_.end())
    return;

  std::map<Connection*, msgid*>::iterator it = clients_.find(_conn);

  msgid* ids = it->second;

  clients_.erase(_conn);

  delete ids;
}

void
OrderServer::HandleOrderMessage
 (
   Connection*   _conn,
   OrderMessage* _message
 )
{
  if (clients_.find(_conn) == clients_.end())
    return;

  nrecv_++;

  std::map<Connection*, msgid*>::iterator it = clients_.find(_conn);

  EXPECT_EQ(it->second->incr_idr(), _message->id());

  _message->set_id(it->second->incr_ids());
  SendMessage(_conn, *_message);

  nsent_++;
}

void OrderServer::Terminate()
{
  Stop();
  getEventLoop()->loopExit();
}

void
OrderServer::HandleTerminateMessage
 (
   Connection*         _connection __attribute__((unused)),
   TerminateMessage*   _message __attribute__((unused))
 )
{
  AddTimer([this] () { this->Terminate(); }, 1);
}
