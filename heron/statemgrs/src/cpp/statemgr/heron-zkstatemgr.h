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

//////////////////////////////////////////////////////////////////////////////
//
// heron-zkstatemgr.h
// Author:- Sanjeev Kulkarni(skulkarni@twitter.com)
//
// This file defines the ZK implenentation of the HeronStateMgr interface.
// The details are
// 1. TopologyManagerLocation is kept as an ephimeral node. This way if the
//    primary goes away, the node is not there. Thus a create would
//    succeed. If there is another tmanager, the createnode would fail
//    which would give the indication that another tmanager was running.
//    Thus TManagerServer can use just the set method to see if he is the
//    only tmanager.
// 2. Once #1 is ensured, Topology and Assignment are straightforward
//    create/set operations
// 3. The Topology node always exists. However the assignment may or
//    may not exist. Currently TManager always does get of assignment
//    to see if some assignment exists or not. We also keep track
//    of this. So that the next time a SetAssignment is called,
//    we know whether to do createnode or setnode
//////////////////////////////////////////////////////////////////////////////
#ifndef __HERON_ZKSTATE_H
#define __HERON_ZKSTATE_H

#include <string>
#include <vector>
#include <utility>

#include "zookeeper/zkclient.h"
#include "zookeeper/zkclient_factory.h"
#include "statemgr/heron-statemgr.h"

class ZKClient;

namespace heron {
namespace common {

class HeronZKStateMgr : public HeronStateMgr {
 public:
  HeronZKStateMgr(const std::string& zkhostport, const std::string& topleveldir,
                  std::shared_ptr<EventLoop> eventLoop, bool exitOnSessionExpiry);
  virtual ~HeronZKStateMgr();

  //
  // Interface implementations
  //

  void InitTree();

  // Sets up a watch on tmanager location change
  void SetTManagerLocationWatch(const std::string& _topology_name, VCallback<> _watcher);
  void SetMetricsCacheLocationWatch(const std::string& _topology_name, VCallback<> _watcher);
  void SetPackingPlanWatch(const std::string& _topology_name, VCallback<> _watcher);

  // Sets the Tmanager
  void SetTManagerLocation(const proto::tmanager::TManagerLocation& _location,
                          VCallback<proto::system::StatusCode> _cb);
  void GetTManagerLocation(const std::string& _topology_name,
                          shared_ptr<proto::tmanager::TManagerLocation> _return,
                          VCallback<proto::system::StatusCode> _cb);
  void SetMetricsCacheLocation(const proto::tmanager::MetricsCacheLocation& _location,
                          VCallback<proto::system::StatusCode> _cb);
  void GetMetricsCacheLocation(const std::string& _topology_name,
                          shared_ptr<proto::tmanager::MetricsCacheLocation> _return,
                          VCallback<proto::system::StatusCode> _cb);

  // Gets/Sets the Topology
  void CreateTopology(const proto::api::Topology& _top, VCallback<proto::system::StatusCode> _cb);
  void DeleteTopology(const std::string& _topology_name, VCallback<proto::system::StatusCode> _cb);
  void SetTopology(const proto::api::Topology& _top, VCallback<proto::system::StatusCode> _cb);
  void GetTopology(const std::string& _topology_name, proto::api::Topology& _return,
                   VCallback<proto::system::StatusCode> _cb);

  // Gets/Sets physical plan
  void CreatePhysicalPlan(const proto::system::PhysicalPlan& _plan,
                          VCallback<proto::system::StatusCode> _cb);
  void DeletePhysicalPlan(const std::string& _topology_name,
                          VCallback<proto::system::StatusCode> _cb);
  void SetPhysicalPlan(const proto::system::PhysicalPlan& _pplan,
                       VCallback<proto::system::StatusCode> _cb);
  void GetPhysicalPlan(const std::string& _topology_name,
                       shared_ptr<proto::system::PhysicalPlan> _return,
                       VCallback<proto::system::StatusCode> _cb);

  void GetPackingPlan(const std::string& _topology_name,
                      shared_ptr<proto::system::PackingPlan> _return,
                      VCallback<proto::system::StatusCode> _cb);

  // Gets/Sets execution state
  void CreateExecutionState(const proto::system::ExecutionState& _state,
                            VCallback<proto::system::StatusCode> _cb);
  void DeleteExecutionState(const std::string& _topology_name,
                            VCallback<proto::system::StatusCode> _cb);
  void GetExecutionState(const std::string& _topology_name, proto::system::ExecutionState* _return,
                         VCallback<proto::system::StatusCode> _cb);
  void SetExecutionState(const proto::system::ExecutionState& _state,
                         VCallback<proto::system::StatusCode> _cb);

  // Gets/Sets the Stateful Checkpoints
  void CreateStatefulCheckpoints(const std::string& _topology_name,
          shared_ptr<proto::ckptmgr::StatefulConsistentCheckpoints> _ckpt,
          VCallback<proto::system::StatusCode> _cb);
  void DeleteStatefulCheckpoints(const std::string& _topology_name,
               VCallback<proto::system::StatusCode> _cb);
  void SetStatefulCheckpoints(const std::string& _topology_name,
               shared_ptr<proto::ckptmgr::StatefulConsistentCheckpoints> _ckpt,
               VCallback<proto::system::StatusCode> _cb);
  void GetStatefulCheckpoints(const std::string& _topology_name,
                              shared_ptr<proto::ckptmgr::StatefulConsistentCheckpoints> _return,
                              VCallback<proto::system::StatusCode> _cb);

  void ListTopologies(std::vector<sp_string>* _return, VCallback<proto::system::StatusCode> _cb);
  void ListExecutionStateTopologies(std::vector<sp_string>* _return,
                                    VCallback<proto::system::StatusCode> _cb);

  virtual std::string GetStateLocation() { return zkhostport_; }

 protected:
  // A test ONLY constructor used to pass a ZKClientFactory which could
  // return a MockZKClient
  HeronZKStateMgr(const std::string& zkhostport, const std::string& topleveldir,
                  std::shared_ptr<EventLoop> eventLoop, ZKClientFactory* zkclient_factory,
                  bool exitOnSessionExpiry = false);

 private:
  // Done methods
  void SetTManagerLocationDone(VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);
  void SetMetricsCacheLocationDone(VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);
  void GetTManagerLocationDone(std::string* _contents,
                              shared_ptr<proto::tmanager::TManagerLocation> _return,
                              VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);
  void GetMetricsCacheLocationDone(std::string* _contents,
                                   shared_ptr<proto::tmanager::MetricsCacheLocation> _return,
                                   VCallback<proto::system::StatusCode> _cb,
                                   sp_int32 _rc);

  void CreateTopologyDone(VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);
  void DeleteTopologyDone(VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);
  void SetTopologyDone(VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);
  void GetTopologyDone(std::string* _contents, proto::api::Topology& _return,
                       VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);

  void CreatePhysicalPlanDone(VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);
  void DeletePhysicalPlanDone(VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);
  void SetPhysicalPlanDone(VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);
  void GetPhysicalPlanDone(std::string* _contents, shared_ptr<proto::system::PhysicalPlan> _return,
                           VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);
  void GetPackingPlanDone(std::string* _contents, shared_ptr<proto::system::PackingPlan> _return,
                          VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);

  void CreateExecutionStateDone(VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);
  void DeleteExecutionStateDone(VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);
  void SetExecutionStateDone(VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);
  void GetExecutionStateDone(std::string* _contents, proto::system::ExecutionState* _return,
                             VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);

  void CreateStatefulCheckpointsDone(VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);
  void DeleteStatefulCheckpointsDone(VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);
  void SetStatefulCheckpointsDone(VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);
  void GetStatefulCheckpointsDone(std::string* _contents,
                           shared_ptr<proto::ckptmgr::StatefulConsistentCheckpoints> _return,
                           VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);

  void ListTopologiesDone(VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);
  void ListExecutionStateTopologiesDone(VCallback<proto::system::StatusCode> _cb, sp_int32 _rc);

  // This is the callback passed to ZkClient, to handle tmanager location
  // changes. It inturn calls the tmanager_location_watcher to notify the
  // clients about the change.
  void TManagerLocationWatch();
  void MetricsCacheLocationWatch();
  void PackingPlanWatch();
  // Handles global events from ZKClient. For now, it handles the session
  // expired event, by deleting the current client, creating a new one,
  // setting the tmanager location watch, and notifying the client of a
  // possible tmanager location change.
  void GlobalWatchEventHandler(const ZKClient::ZkWatchEvent event);
  // Sets a tmanager location watch through the ZKClient Exists method.
  void SetTManagerLocationWatchInternal();
  void SetMetricsCacheLocationWatchInternal();
  void SetPackingPlanWatchInternal();
  // A wrapper to be passed to select server registerTimer call.
  // Ignores the status and call SetTManagerLocationWatchInternal
  void CallSetTManagerLocationWatch(EventLoop::Status status);
  void CallSetMetricsCacheLocationWatch(EventLoop::Status status);
  void CallSetPackingPlanWatch(EventLoop::Status status);
  // A handler callback that gets called by ZkClient upon completion of
  // setting Tmanager watch. If the return code indicates failure, we
  // retry after SET_WATCH_RETRY_INTERVAL_S seconds.
  void SetTManagerWatchCompletionHandler(sp_int32 rc);
  void SetMetricsCacheWatchCompletionHandler(sp_int32 rc);
  void SetPackingPlanWatchCompletionHandler(sp_int32 rc);
  // Essentially tells you whether SetTmanagerLocationWatch has been
  // called by the client or not. It gets this info through
  // tmanager_location_watcher_info_
  bool IsTmanagerWatchDefined();
  bool IsMetricsCacheWatchDefined();
  bool IsPackingPlanWatchDefined();
  // Common functionality for c`tors. Should be called only once from c`tor
  void Init();

  // Tells if the failure of setting zk node watch is retryable.
  // Currently returns true on connection related errors
  static bool ShouldRetrySetWatch(sp_int32 rc);

  const std::string zkhostport_;
  ZKClient* zkclient_;
  // A factory for creating ZKClient. It defaults to DefaultZKClientFactory.
  // For tests it could be overriden to a factor that returns a MockZkClient
  // This class owns the factory, and is responsible for deleting it.
  ZKClientFactory* const zkclient_factory_;
  std::shared_ptr<EventLoop> eventLoop_;
  // A permanent callback initialized to wrap the WatchEventHandler
  VCallback<ZKClient::ZkWatchEvent> watch_event_cb_;

  // Holds the tmanager location watch callback and the topology name
  // passed by the client. Needed for recreating tmanager location watch
  // on session expiry. Only set after 'SetTmanagerLocationWatch' method
  // is called.
  struct TManagerLocationWatchInfo {
    VCallback<> watcher_cb;
    std::string topology_name;

    TManagerLocationWatchInfo(VCallback<> watcher, std::string name)
        : watcher_cb(std::move(watcher)), topology_name(name) {}
  };

  const TManagerLocationWatchInfo* tmanager_location_watcher_info_;
  const TManagerLocationWatchInfo* metricscache_location_watcher_info_;
  const TManagerLocationWatchInfo* packing_plan_watcher_info_;
  // If true, we exit on zookeeper session expired event
  const bool exitOnSessionExpiry_;
  // Retry interval if setting a watch on zk node fails.
  static const sp_int32 SET_WATCH_RETRY_INTERVAL_S;
  // For easier unit testing, to allow access to private methods.
  friend class HeronZKStateMgrTest;
};
}  // namespace common
}  // namespace heron

#endif
