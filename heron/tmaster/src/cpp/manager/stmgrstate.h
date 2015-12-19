#ifndef __STMANAGERSTATE_H
#define __STMANAGERSTATE_H

#include <string>
#include <vector>

namespace heron { namespace proto { namespace system {
  class StMgr;
  class StMgrStats;
  class PhysicalPlan;
}}}

namespace heron { namespace tmaster {

class TMasterServer;

class StMgrState
{
 public:
  StMgrState(Connection* _conn, const proto::system::StMgr& _info,
             const std::vector<proto::system::Instance*>& _instances,
             TMasterServer* _server);
  virtual ~StMgrState();

  void UpdateWithNewStMgr(const proto::system::StMgr& _info,
                          const std::vector<proto::system::Instance*>& _instances,
                          Connection* _conn);

  // Update the heartbeat. Note:- We own _stats now
  void heartbeat(sp_int64 _time, proto::system::StMgrStats* _stats);

  // Send messages to the stmgr
  void NewPhysicalPlan(const proto::system::PhysicalPlan& _pplan);

  bool TimedOut() const;

  // getters
  Connection* get_connection() { return connection_; }
  const std::string& get_id() const { return stmgr_->id(); }
  sp_uint32 get_num_instances() const { return instances_.size(); }
  const std::vector<proto::system::Instance*>& get_instances() const { return instances_; }
  const proto::system::StMgr* get_stmgr() const { return stmgr_; }
  bool VerifyInstances(const std::vector<proto::system::Instance*>& _instances);

 private:
  // The last time we got a hearbeat from this stmgr
  sp_int64                                  last_heartbeat_;
  // The stats that was reported last time
  proto::system::StMgrStats*                last_stats_;

  // All the instances on this stmgr
  std::vector<proto::system::Instance*>          instances_;

  // The info about this stmgr
  proto::system::StMgr*                     stmgr_;
  // The connection used by the nodemanager to contact us
  Connection*                               connection_;
  // Our link to our TMaster
  TMasterServer*                            server_;
};

}} // end namespace

#endif
