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
// heron-localfilestatemgr.h
// Author:- Sanjeev Kulkarni(skulkarni@twitter.com)
//
// This file implements the HeronStateMgr interface using local file system
// as the storage device. This is useful when running heron in simulator
// or for testing purposes.
// Currently every different piece of state is stored in different files
// as a dump of the proto structure.
// NOTE:- The implementation is blocking. We dont to async file operations
// for reading/writing state information. This is ok for simulator.
//////////////////////////////////////////////////////////////////////////////
#ifndef __HERON_LOCALFILESTATEMGR_H
#define __HERON_LOCALFILESTATEMGR_H

#include <string>
#include <vector>
#include "statemgr/heron-statemgr.h"

namespace heron {
namespace common {

class HeronLocalFileStateMgr : public HeronStateMgr {
 public:
  HeronLocalFileStateMgr(const std::string& _topleveldir, EventLoop* eventLoop);
  virtual ~HeronLocalFileStateMgr();

  // Sets up the basic filesystem tree at the given location
  void InitTree();

  void SetTMasterLocationWatch(const std::string& _topology_name, VCallback<> _watcher);
  void SetMetricsCacheLocationWatch(const std::string& _topology_name, VCallback<> _watcher);

  // implement the functions
  void GetTMasterLocation(const std::string& _topology_name,
                          proto::tmaster::TMasterLocation* _return,
                          VCallback<proto::system::StatusCode> _cb);
  void SetTMasterLocation(const proto::tmaster::TMasterLocation& _location,
                          VCallback<proto::system::StatusCode> _cb);
  void GetMetricsCacheLocation(const std::string& _topology_name,
                          proto::tmaster::MetricsCacheLocation* _return,
                          VCallback<proto::system::StatusCode> _cb);
  void SetMetricsCacheLocation(const proto::tmaster::MetricsCacheLocation& _location,
                          VCallback<proto::system::StatusCode> _cb);

  void CreateTopology(const proto::api::Topology& _top, VCallback<proto::system::StatusCode> _cb);
  void DeleteTopology(const std::string& _topology_name, VCallback<proto::system::StatusCode> _cb);
  void SetTopology(const proto::api::Topology& _top, VCallback<proto::system::StatusCode> _cb);
  void GetTopology(const std::string& _topology_name, proto::api::Topology* _return,
                   VCallback<proto::system::StatusCode> _cb);

  void CreatePhysicalPlan(const proto::system::PhysicalPlan& _pplan,
                          VCallback<proto::system::StatusCode> _cb);
  void DeletePhysicalPlan(const std::string& _topology_name,
                          VCallback<proto::system::StatusCode> _cb);
  void SetPhysicalPlan(const proto::system::PhysicalPlan& _pplan,
                       VCallback<proto::system::StatusCode> _cb);
  void GetPhysicalPlan(const std::string& _topology_name, proto::system::PhysicalPlan* _return,
                       VCallback<proto::system::StatusCode> _cb);

  void CreateExecutionState(const proto::system::ExecutionState& _pplan,
                            VCallback<proto::system::StatusCode> _cb);
  void DeleteExecutionState(const std::string& _topology_name,
                            VCallback<proto::system::StatusCode> _cb);
  void GetExecutionState(const std::string& _topology_name, proto::system::ExecutionState* _return,
                         VCallback<proto::system::StatusCode> _cb);
  void SetExecutionState(const proto::system::ExecutionState& _state,
                         VCallback<proto::system::StatusCode> _cb);

  void ListTopologies(std::vector<sp_string>* _return, VCallback<proto::system::StatusCode> _cb);
  void ListExecutionStateTopologies(std::vector<sp_string>* _return,
                                    VCallback<proto::system::StatusCode> _cb);

  virtual sp_string GetStateLocation() { return "LOCALMODE"; }

 private:
  // helper function to get all the file's contents
  proto::system::StatusCode ReadAllFileContents(const std::string& _filename,
                                                std::string& _contents);

  // helper function to write out the contents to a file
  proto::system::StatusCode WriteToFile(const std::string& _filename, const std::string& _contents);

  // helper function to delete a file
  proto::system::StatusCode DeleteFile(const std::string& _filename);

  // helper function to see if a file exists
  proto::system::StatusCode MakeSureFileDoesNotExist(const std::string& _filename);

  // helper function to see if the tmaster location has changed
  void CheckTMasterLocation(std::string _topology_name, time_t _last_change, VCallback<> _watcher,
                            EventLoop::Status);
  void CheckMetricsCacheLocation(std::string _topology_name, time_t _last_change,
                                 VCallback<> _watcher, EventLoop::Status);

  // Hold the EventLoop for scheduling callbacks
  EventLoop* eventLoop_;
};
}  // namespace common
}  // namespace heron

#endif
