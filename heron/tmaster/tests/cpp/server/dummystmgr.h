#ifndef __DUMMYSTMGR_H_
#define __DUMMYSTMGR_H_

#include <string>
#include <vector>
#include "network/network_error.h"

namespace heron { namespace proto {
  namespace tmaster {
    class StMgrRegisterResponse;
    class StMgrHeartbeatResponse;
  }
  namespace stmgr {
    class NewPhysicalPlanMessage;
  }
}}

namespace heron { namespace testing {

class DummyStMgr : public Client
{
 public:
  DummyStMgr(EventLoop* eventLoop, const NetworkOptions& options,
             const sp_string& stmgr_id, const sp_string& myhost,
             sp_int32 myport, const std::vector<proto::system::Instance*>& instances);
  ~DummyStMgr();

  proto::system::PhysicalPlan* GetPhysicalPlan();

 protected:
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

 private:
  void HandleRegisterResponse(void*,
                              proto::tmaster::StMgrRegisterResponse* response,
                              NetworkErrorCode);
  void HandleHeartbeatResponse(void*,
                               proto::tmaster::StMgrHeartbeatResponse* response,
                               NetworkErrorCode);
  void HandleNewAssignmentMessage(proto::stmgr::NewPhysicalPlanMessage* message);
  void HandleNewPhysicalPlan(const proto::system::PhysicalPlan& pplan);

  void OnReConnectTimer();
  void OnHeartbeatTimer();
  void SendRegisterRequest();
  void SendHeartbeatRequest();

  std::string      my_id_;
  std::string      my_host_;
  sp_int32    my_port_;
  std::vector<proto::system::Instance*> instances_;

  proto::system::PhysicalPlan* pplan_;
};

}} // end namespace

#endif
