/*
 * Copyright 2017 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_STATEFUL_RESTORER_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_STATEFUL_RESTORER_H_

#include <ostream>
#include <map>
#include <set>
#include <string>
#include <vector>
#include <typeinfo>   // operator typeid
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"
#include "grouping/shuffle-grouping.h"

namespace heron {
namespace proto {
namespace system {
class PhysicalPlan;
}
}
namespace common {
class MetricsMgrSt;
class MultiCountMetric;
class TimeSpentMetric;
}
}  // namespace heron

namespace heron {
namespace stmgr {

class StMgrServer;
class TupleCache;
class StMgrClientMgr;
class CkptMgrClient;

class StatefulRestorer {
 public:
  explicit StatefulRestorer(CkptMgrClient* _ckptmgr,
                            StMgrClientMgr* _clientmgr, TupleCache* _tuple_cache,
                            StMgrServer* _server,
                            common::MetricsMgrSt* _metrics_manager_client,
                            std::function<void(proto::system::StatusCode,
                                               std::string, sp_int64)> _restore_done_watcher);
  virtual ~StatefulRestorer();
  // Called when stmgr receives a RestoreTopologyStateRequest message
  void StartRestore(const std::string& _checkpoint_id, sp_int64 _restore_txid,
                    proto::system::PhysicalPlan* _pplan);
  // Called when ckptmgr client restarts
  void HandleCkptMgrRestart();
  // Called when instance responds back with RestoredInstanceStateResponse
  void HandleInstanceRestoredState(sp_int32 _task_id,
                                   const proto::system::StatusCode _status,
                                   const std::string& _checkpoint_id);
  // called when ckptmgr returns with instance state
  void HandleCheckpointState(proto::system::StatusCode _status, sp_int32 _task_id,
                             sp_string _checkpoint_id,
                             const proto::ckptmgr::InstanceStateCheckpoint& _state);
  // called when a stmgr connection closes
  void HandleDeadStMgrConnection();
  // called when all clients get connected
  void HandleAllStMgrClientsConnected();
  // called when an instance is dead
  void HandleDeadInstanceConnection(sp_int32 _task_id);
  // called when all instances are connected
  void HandleAllInstancesConnected();
  bool InProgress() const { return in_progress_; }

 private:
  void GetCheckpoints();
  void CheckAndFinishRestore();

  // Set of task ids for which we have sent out get checkpoint requests
  // to the ckptmgr but haven't gotten response back
  std::set<sp_int32> get_ckpt_pending_;
  // Set of task ids which still haven;'t gotten back after
  // restoring their state
  std::set<sp_int32> restore_pending_;
  // Have we not connected with some of our peer stmgrs?
  bool clients_connections_pending_;
  // Are any of our local instances still not connected with us?
  bool instance_connections_pending_;
  // What is the checkpoint id that we are currently restoring
  std::string checkpoint_id_;
  // What is the restore txid associated with our attempt of restoring
  sp_int64 restore_txid_;
  // What are all our local taskids that we need to restore
  std::set<sp_int32> local_taskids_;

  CkptMgrClient* ckptmgr_;
  StMgrClientMgr* clientmgr_;
  TupleCache* tuple_cache_;
  StMgrServer* server_;
  common::MetricsMgrSt* metrics_manager_client_;

  // Are we in the middle of a restore
  bool in_progress_;
  // We will call this function after we are done restoring. We pass
  // the status(whether we were successfull in restoring all our components),
  // the ckpt id that we restored and the restore txid
  std::function<void(proto::system::StatusCode, std::string, sp_int64)> restore_done_watcher_;

  // Different metrics
  common::MultiCountMetric* multi_count_metric_;
  common::TimeSpentMetric* time_spent_metric_;
};
}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STATEFUL_RESTORER_H_
