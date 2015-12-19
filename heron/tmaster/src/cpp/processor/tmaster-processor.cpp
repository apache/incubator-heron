#include <iostream>

#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "processor/tmaster-processor.h"

namespace heron { namespace tmaster {

Processor::Processor(REQID _reqid,
                     Connection* _conn, 
                     google::protobuf::Message* _request,
                     TMaster* _tmaster,
                     Server* _server)
 : request_(_request), tmaster_(_tmaster), server_(_server),
   reqid_(_reqid), conn_(_conn)
{
}

Processor::~Processor()
{
  delete request_;
}

void
Processor::SendResponse(const google::protobuf::Message& _response)
{
  server_->SendResponse(reqid_, conn_, _response);
}

void
Processor::CloseConnection()
{
  server_->CloseConnection(conn_);
}

}} // end namespace
