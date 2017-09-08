#include "network/misc/echoserver.h"
#include <iostream>
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "proto/messages.h"

EchoServer::EchoServer(EventLoopImpl* eventLoop, const NetworkOptions& _options)
  : Server(eventLoop, _options)
{
  InstallMessageHandler(&EchoServer::HandleEchoMessage);
}

EchoServer::~EchoServer()
{
}

void EchoServer::HandleNewConnection(Connection*)
{
  std::cout << "EchoServer accepting new connection\n";
}

void EchoServer::HandleConnectionClose(Connection*, NetworkErrorCode _status)
{
  std::cout << "Connection dropped from echoserver with status " << _status << "\n";
}

void EchoServer::HandleEchoMessage(Connection* conn, heron::proto::system::RootId* message) {
  std::cout << "Got a rootid request " << message->taskid() << std::endl;
  delete message;
  heron::proto::system::RootId msg;
  msg.set_taskid(1);
  msg.set_key(112);
  SendMessage(conn, msg);
}
