#include <iostream>
#include <stdio.h>
#include <string.h>

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "network/unittests.pb.h"
#include "network/host_unittest.h"
#include "network/client_unittest.h"

TestClient::TestClient
 (
   EventLoopImpl*         eventLoop,
   const NetworkOptions& _options,
   sp_uint64             _ntotal
 ) : Client(eventLoop, _options), ntotal_(_ntotal)
{
  InstallMessageHandler(&TestClient::HandleTestMessage);
  start_time_ = time(NULL);
  nsent_ = nrecv_ = 0;

  // Setup the call back function to be invoked when retrying
  retry_cb_ = [this] () { this->Retry(); };
}

TestClient::~TestClient()
{
}

void
TestClient::CreateAndSendMessage()
{
  TestMessage* message = new TestMessage();

  for (sp_int32 i = 0; i < 100; ++i)
  {
    message->add_message("some message");
  }

  SendMessage(message);
  return;
}

void
TestClient::HandleConnect(NetworkErrorCode _status)
{
  if (_status == OK)
  {
    SendMessages();
  }
  else
  {
    // Retry after some time
    AddTimer(retry_cb_, RETRY_TIMEOUT);
  }
}

void
TestClient::HandleClose(NetworkErrorCode)
{
}

void
TestClient::HandleTestMessage(TestMessage* _message)
{
  ++nrecv_;
  delete _message;

  if (nrecv_ >= ntotal_)
  {
    Stop();
    getEventLoop()->loopExit();
  }
}

void
TestClient::SendMessages()
{
  while (getOutstandingPackets() < 10000)
  {
    CreateAndSendMessage();
    if (++nsent_ >= ntotal_) {
      return ;
    }
  }

  // every ms - call send messages
  AddTimer([this] () { this->SendMessages(); }, 1000);
}
