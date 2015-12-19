#include "core/network/misc/echoserver.h"
#include <iostream>

EchoServer::EchoServer(EventLoopImpl* eventLoop, const NetworkOptions& _options)
  : Server(eventLoop, _options)
{
  InstallRequestHandler(&EchoServer::HandleEchoRequest);
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

void EchoServer::HandleEchoRequest(REQID _id, Connection* _connection,
                                   EchoServerRequest* _request)
{
  // cout << "Got a echo request " << _request->echo_request() << endl;
  EchoServerResponse response;
  response.set_echo_response(_request->echo_request());
  SendResponse(_id, _connection, response);
  delete _request;
}
