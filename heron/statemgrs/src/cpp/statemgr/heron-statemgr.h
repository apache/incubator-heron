/*
 * Copyright 2015 Twitter, Inc.
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

//////////////////////////////////////////////////////////////////////////////
//
// heron-statemgr.h
// Author:- Sanjeev Kulkarni(skulkarni@twitter.com)
//
// This file defines the HeronStateMgr interface.
// Services accross Heron use HeronStateMgr to get/set state information.
// Currently the primary things kept by state are
// 1. Where is the the topology master running.
//    The topology master is responsible for writing this information out
//    upon startup. The streammgrs query this upon startup to find out
//    who is their topology master. In case they loose connection with
//    the topology master, the streammgrs query this again to see
//    if the topology master has changed.
// 2. Topology and the current running state of the topology
//    This information is seeded by the topology submitter.
//    The topology master updates this when the state of the topology
//    changes.
// 3. Current assignment.
//    This information is solely used by topology master. When it
//    creates a new assignment or when the assignment changes, it writes
//    out this information. This is required for topology master failover.
//
// Clients call the methods of the state passing a callback. The callback
// is called with result code upon the completion of the operation.
//////////////////////////////////////////////////////////////////////////////
#ifndef __HERON_STATE_H
#define __HERON_STATE_H

#include <string>
#include <vector>
#include "basics/basics.h"
#include "network/network.h"
#include "proto/messages.h"

namespace heron {
namespace common {

class HeronStateMgr {
 public:
  explicit HeronStateMgr(const std::string& _topleveldir);
  virtual ~HeronStateMgr();

  // Factory method to create
  static HeronStateMgr* MakeStateMgr(const std::string& _zk_hostport,
                                     const std::string& _topleveldir, EventLoop* eventLoop,
                                     bool exitOnSessionExpiry = true);

  //
  // Interface methods
  //

  // Sets up the basic tree for zk or filesystem
  virtual void InitTree() = 0;

  // Sets up a watch on tmaster location change
  // Everytime there is a change in tmaster location, _watcher
  // will be called. Users dont need to be bothered about
  // registering the watcher again as that will be done by us.
  virtual void SetTMasterLocationWatch(const std::string& _topology_name, VCallback<> _watcher) = 0;
  virtual void SetMetricsCacheLocationWatch(
               const std::string& _topology_name, VCallback<> _watcher) = 0;

  // Sets/Gets the Tmaster
  virtual void GetTMasterLocation(const std::string& _topology_name,
                                  proto::tmaster::TMasterLocation* _return,
                                  VCallback<proto::system::StatusCode> _cb) = 0;
  virtual void SetTMasterLocation(const proto::tmaster::TMasterLocation& _location,
                                  VCallback<proto::system::StatusCode> _cb) = 0;
  virtual void GetMetricsCacheLocation(const std::string& _topology_name,
                                  proto::tmaster::MetricsCacheLocation* _return,
                                  VCallback<proto::system::StatusCode> _cb) = 0;
  virtual void SetMetricsCacheLocation(const proto::tmaster::MetricsCacheLocation& _location,
                                  VCallback<proto::system::StatusCode> _cb) = 0;

  // Gets/Sets the Topology
  virtual void CreateTopology(const proto::api::Topology& _top,
                              VCallback<proto::system::StatusCode> _cb) = 0;
  virtual void DeleteTopology(const std::string& _topology_name,
                              VCallback<proto::system::StatusCode> _cb) = 0;
  virtual void SetTopology(const proto::api::Topology& _top,
                           VCallback<proto::system::StatusCode> _cb) = 0;
  virtual void GetTopology(const std::string& _topology_name, proto::api::Topology* _return,
                           VCallback<proto::system::StatusCode> _cb) = 0;

  // Gets/Sets PhysicalPlan
  virtual void CreatePhysicalPlan(const proto::system::PhysicalPlan& _plan,
                                  VCallback<proto::system::StatusCode> _cb) = 0;
  virtual void DeletePhysicalPlan(const std::string& _topology_name,
                                  VCallback<proto::system::StatusCode> _cb) = 0;
  virtual void SetPhysicalPlan(const proto::system::PhysicalPlan& _plan,
                               VCallback<proto::system::StatusCode> _cb) = 0;
  virtual void GetPhysicalPlan(const std::string& _topology_name,
                               proto::system::PhysicalPlan* _return,
                               VCallback<proto::system::StatusCode> _cb) = 0;

  // Gets/Sets ExecutionState
  virtual void CreateExecutionState(const proto::system::ExecutionState& _st,
                                    VCallback<proto::system::StatusCode> _cb) = 0;
  virtual void DeleteExecutionState(const std::string& _topology_name,
                                    VCallback<proto::system::StatusCode> _cb) = 0;
  virtual void GetExecutionState(const std::string& _topology_name,
                                 proto::system::ExecutionState* _return,
                                 VCallback<proto::system::StatusCode> _cb) = 0;
  virtual void SetExecutionState(const proto::system::ExecutionState& _st,
                                 VCallback<proto::system::StatusCode> _cb) = 0;
  virtual void ListExecutionState(const std::vector<sp_string>& _topologies,
                                  std::vector<proto::system::ExecutionState*>* _return,
                                  VCallback<proto::system::StatusCode> _cb);

  // Calls to list the topologies and physical plans
  virtual void ListTopologies(std::vector<sp_string>* _return,
                              VCallback<proto::system::StatusCode> _cb) = 0;
  // Calls to list the topology names that have execution state
  virtual void ListExecutionStateTopologies(std::vector<sp_string>* _return,
                                            VCallback<proto::system::StatusCode> _cb) = 0;

  virtual std::string GetStateLocation() = 0;
  std::string GetTopLevelDir() { return topleveldir_; }

 protected:
  //
  // We define methods of where the records have to be placed
  //
  std::string GetTMasterLocationPath(const std::string& _topology_name);
  std::string GetMetricsCacheLocationPath(const std::string& _topology_name);
  std::string GetTopologyPath(const std::string& _topology_name);
  std::string GetPhysicalPlanPath(const std::string& _topology_name);
  std::string GetExecutionStatePath(const std::string& _topology_name);

  std::string GetTMasterLocationDir();
  std::string GetMetricsCacheLocationDir();
  std::string GetTopologyDir();
  std::string GetPhysicalPlanDir();
  std::string GetExecutionStateDir();

 private:
  void ListExecutionStateDone(std::vector<proto::system::ExecutionState*>* _return,
                              size_t _required_size, proto::system::ExecutionState* _s,
                              VCallback<proto::system::StatusCode> _cb,
                              proto::system::StatusCode _status);
  std::string topleveldir_;
};
}  // namespace common
}  // namespace heron

#endif
