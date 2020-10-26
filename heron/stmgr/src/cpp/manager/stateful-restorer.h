/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_STATEFUL_RESTORER_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_STATEFUL_RESTORER_H_

#include <ostream>
#include <map>
#include <string>
#include <unordered_set>
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

using std::shared_ptr;

class InstanceServer;
class TupleCache;
class StMgrClientMgr;
class CkptMgrClient;

// For Heron topologies running in effectively once semantics, the tmanager
// could initiate restore topology to a certain globally consistent checkpoint.
// This could be triggered either during startup or after failure of certain
// topology components. StatefulRestorer implements the state machine of this recovery
// process inside the stmgr. When stmgr receives the request to restore the topology
// to a specific checkpoint, it starts the statemachine by invoking the
// StartRestore. The main task of the restore state machine is to:-
// 1. Clear all internal caches(like tuple cache, checkpoint_gateway buffer, etc)
// 2. Retreive State of all local instances via the checkpoint manager
// 3. Sending RestoreState request with the retreived state to all local instances
// 4. Upon completion of recovery process, calling the _restore_done_watcher
// Note that while all of this is in process, instances could die, stmgr clients
// might get disconnected, checkpoint manager could disappear, etc. These events
// are signalled to the restorer by the stmgr invoking the appopriate methods
// of the StatefulRestorer(HandleDeadInstanceConnection, HandleAllInstancesConnected,
// HandleDeadStMgrConnection, HandleAllStMgrClientsConnected, HandleCkptMgrRestart).
// The restorer will update its state based on these unexpected failures occuring.
//
// For more information please refer to the stateful processing design doc at
// https://docs.google.com/document/d/1pNuE77diSrYHb7vHPuPO3DZqYdcxrhywH_f7loVryCI/edit#
// and in particular the recovery section.
class StatefulRestorer {
 public:
  explicit StatefulRestorer(shared_ptr<CkptMgrClient> _ckptmgr,
                            shared_ptr<StMgrClientMgr> _clientmgr,
                            shared_ptr<TupleCache> _tuple_cache,
                            shared_ptr<InstanceServer> _server,
                            shared_ptr<common::MetricsMgrSt> const& _metrics_manager_client,
                            std::function<void(proto::system::StatusCode,
                                               std::string, sp_int64)> _restore_done_watcher);
  virtual ~StatefulRestorer();
  // Called when stmgr receives a RestoreTopologyStateRequest message. This is the
  // beginning of the restore state machine inside the stmgr. This is a request
  // to clear all caches and restore the state of all instances to the
  // _checkpoint_id checkpoint. The _pplan represents the physical plan
  // at the start of the restore. Upon the completion of the restore process(either
  // successfully or otherwise), the restorer calls the _restore_done_watcher
  // callback passing in it the status of the restore along with _checkpoint_id
  // and the _restore_txid
  void StartRestore(const std::string& _checkpoint_id, sp_int64 _restore_txid,
                    const std::unordered_set<sp_int32>& _local_taskids,
                    proto::system::PhysicalPlan const& _pplan);
  // Called when ckptmgr client restarts
  void HandleCkptMgrRestart();
  // Called when local instance _task_id is done restoring its state at
  // _checkpoint_id. If the restoration was successful, this means that this
  // instance is good to go to start processing. After all the local instances
  // have restored, the _restore_done_watcher callback is invoked signalling
  // the completion of the recovery
  void HandleInstanceRestoredState(sp_int32 _task_id,
                                   const proto::system::StatusCode _status,
                                   const std::string& _checkpoint_id);
  // called when ckptmgr retrieves the instance state at _checkpoint_id.
  // We need to send this state to the local task _task_id to have it
  // restore its state pointed to by _state.
  void HandleCheckpointState(proto::system::StatusCode _status, sp_int32 _task_id,
                             sp_string _checkpoint_id,
                             const proto::ckptmgr::InstanceStateCheckpoint& _state);
  // called when a stmgr connection closes. If we are in the middle of a restore,
  // we cannot complete it until all our stmgr clients are connected
  void HandleDeadStMgrConnection();
  // called when all the stmgr clients get connected
  void HandleAllStMgrClientsConnected();
  // called when an instance is dead. If we are in the middle of a restore,
  // we need to wait till it comes back and is restored to its appropriate state.
  void HandleDeadInstanceConnection(sp_int32 _task_id);
  // called when all instances are connected
  void HandleAllInstancesConnected();
  bool InProgress() const { return in_progress_; }

 private:
  void GetCheckpoints();
  void CheckAndFinishRestore();

  // Set of task ids for which we have sent out get checkpoint requests
  // to the ckptmgr but haven't gotten response back
  std::unordered_set<sp_int32> get_ckpt_pending_;
  // Set of task ids which still haven;'t gotten back after
  // restoring their state
  std::unordered_set<sp_int32> restore_pending_;
  // Have we not connected with some of our peer stmgrs?
  bool clients_connections_pending_;
  // Are any of our local instances still not connected with us?
  bool instance_connections_pending_;
  // What is the checkpoint id that we are currently restoring
  std::string checkpoint_id_;
  // What is the restore txid associated with our attempt of restoring
  sp_int64 restore_txid_;
  // What are all our local taskids that we need to restore
  std::unordered_set<sp_int32> local_taskids_;

  shared_ptr<CkptMgrClient> ckptmgr_;
  shared_ptr<StMgrClientMgr> clientmgr_;
  shared_ptr<TupleCache> tuple_cache_;
  shared_ptr<InstanceServer> server_;
  shared_ptr<common::MetricsMgrSt> metrics_manager_client_;

  // Are we in the middle of a restore
  bool in_progress_;
  // We will call this function after we are done restoring. We pass
  // the status(whether we were successfull in restoring all our components),
  // the ckpt id that we restored and the restore txid
  std::function<void(proto::system::StatusCode, std::string, sp_int64)> restore_done_watcher_;

  // Different metrics
  shared_ptr<common::MultiCountMetric> multi_count_metric_;
  shared_ptr<common::TimeSpentMetric>  time_spent_metric_;
};
}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STATEFUL_RESTORER_H_
