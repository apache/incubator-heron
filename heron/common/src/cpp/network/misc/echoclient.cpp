#include "network/misc/echoclient.h"
#include <stdio.h>
#include <iostream>

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "proto/messages.h"

EchoClient::EchoClient(EventLoopImpl* eventLoop, const NetworkOptions& _options)
  : Client(eventLoop, _options), nrequests_(0) {
  InstallMessageHandler(&EchoClient::HandleEchoMessage);
}

EchoClient::~EchoClient() {
}

void EchoClient::CreateAndSendMessage() {
  heron::proto::system::RootId message;
  message.set_taskid(1);
  message.set_key(111);
  SendMessage(message);
  nrequests_++;
  return;
}

void EchoClient::HandleConnect(NetworkErrorCode _status) {
  if (_status == OK) {
    std::cout << "Connected to " << get_clientoptions().get_host()
         << ":" << get_clientoptions().get_port() << std::endl;
    CreateAndSendMessage();
  } else {
    std::cout << "Could not connect to " << get_clientoptions().get_host()
         << ":" << get_clientoptions().get_port() << std::endl;
    Stop();
  }
}

void EchoClient::HandleClose(NetworkErrorCode) {
  std::cout << "Server connection closed\n";
  getEventLoop()->loopExit();
}

void EchoClient::HandleEchoMessage(heron::proto::system::RootId* message) {
  std::cout << "Got a message in the client" << std::endl;
  delete message;
  CreateAndSendMessage();
}
