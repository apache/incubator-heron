#include <iostream>

#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "manager/tmaster.h"
#include "processor/tmaster-processor.h"
#include "processor/stmgr-heartbeat-processor.h"

namespace heron { namespace tmaster {

StMgrHeartbeatProcessor::StMgrHeartbeatProcessor(
  REQID                                    reqid,
  Connection*                              conn, 
  proto::tmaster::StMgrHeartbeatRequest*   request,
  TMaster*                                 tmaster,
  Server*                                  server)
  : Processor(reqid, conn, request, tmaster, server)
{
}

StMgrHeartbeatProcessor::~StMgrHeartbeatProcessor()
{
  // nothing to be done here
}

void
StMgrHeartbeatProcessor::Start()
{
  proto::tmaster::StMgrHeartbeatRequest* request = 
            static_cast<proto::tmaster::StMgrHeartbeatRequest*>(request_);

  proto::system::Status* status = 
              tmaster_->UpdateStMgrHeartbeat(GetConnection(),
                                             request->heartbeat_time(),
                                             request->release_stats());
  
  proto::tmaster::StMgrHeartbeatResponse response;              
  response.set_allocated_status(status);
  SendResponse(response);
  delete this;
}

}} // end namespace
