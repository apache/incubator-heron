#include <iostream>
#include <vector>

#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "manager/tmaster.h"
#include "processor/tmaster-processor.h"
#include "processor/stmgr-register-processor.h"

namespace heron { namespace tmaster {

StMgrRegisterProcessor::StMgrRegisterProcessor(REQID _reqid,
                            Connection* _conn,
                            proto::tmaster::StMgrRegisterRequest* _request,
                            TMaster* _tmaster,
                            Server* _server)
 : Processor(_reqid, _conn, _request, _tmaster, _server)
{
}

StMgrRegisterProcessor::~StMgrRegisterProcessor()
{
  // nothing to be done here
}

void
StMgrRegisterProcessor::Start()
{
  // We got a new stream manager registering to us
  // Get the relevant info and ask tmaster to register
  proto::tmaster::StMgrRegisterRequest* request =
             static_cast<proto::tmaster::StMgrRegisterRequest*>(request_);
  std::vector<proto::system::Instance*> instances;
  for (sp_int32 i = 0; i < request->instances_size(); ++i) {
    proto::system::Instance* instance = new proto::system::Instance();
    instance->CopyFrom(request->instances(i));
    instances.push_back(instance);
  }
  proto::system::PhysicalPlan* pplan = NULL;

  proto::system::Status* status =
         tmaster_->RegisterStMgr(request->stmgr(), instances,
                                 GetConnection(), pplan);

  // Send the response
  proto::tmaster::StMgrRegisterResponse response;
  response.set_allocated_status(status);
  if (status->status() == proto::system::OK) {
    if (pplan) {
      response.mutable_pplan()->CopyFrom(*pplan);
    }
  }
  SendResponse(response);
  delete this;
  return;
}

}} // end namespace
