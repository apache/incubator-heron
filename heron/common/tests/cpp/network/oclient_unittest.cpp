#include <iostream>
#include <stdio.h>
#include <string.h>

#include "gtest/gtest.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "network/unittests.pb.h"
#include "network/host_unittest.h"
#include "network/oclient_unittest.h"

OrderClient::OrderClient
 (
   EventLoopImpl*         eventLoop,
   const NetworkOptions& _options,
   sp_uint64             _ntotal
 ) : Client(eventLoop, _options), ntotal_(_ntotal)
{
  InstallMessageHandler(&OrderClient::HandleOrderMessage);
  start_time_ = time(NULL);
  nsent_ = nrecv_ = msgids_ = msgidr_ = 0;

  // Setup the call back function to be invoked when retrying
  retry_cb_ = [this] () { this->Retry(); };
}

void
OrderClient::CreateAndSendMessage()
{
  OrderMessage* message = new OrderMessage();

  message->set_id(msgids_++);

  SendMessage(message);
  return;
}

void
OrderClient::HandleConnect(NetworkErrorCode _status)
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
OrderClient::HandleClose(NetworkErrorCode)
{
}

void
OrderClient::HandleOrderMessage(OrderMessage* _message)
{
  ++nrecv_;

  EXPECT_EQ(msgidr_++, _message->id());

  delete _message;

  if (nrecv_ >= ntotal_)
  {
    Stop();
    getEventLoop()->loopExit();
  }
}

void
OrderClient::SendMessages()
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
