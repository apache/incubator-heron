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

#ifndef __DUMMY_STMGR_CLIENTMGR_H
#define __DUMMY_STMGR_CLIENTMGR_H

#include <string>

#include "proto/messages.h"
#include "manager/stmgr-clientmgr.h"

class DummyStMgrClientMgr : public heron::stmgr::StMgrClientMgr {
 public:
  DummyStMgrClientMgr(std::shared_ptr<EventLoop> _eventLoop,
                      std::shared_ptr<heron::common::MetricsMgrSt> const& _metrics,
                      const std::string& _stmgr,
                      heron::proto::system::PhysicalPlan* _pplan)
  : heron::stmgr::StMgrClientMgr(_eventLoop, _pplan->topology().name(),
                                 _pplan->topology().id(), _stmgr,
                                 NULL, _metrics, 1024, 2048, false),
    close_connections_called_(false), start_connections_called_(false),
    all_stmgrclients_registered_(false) {
  }

  virtual ~DummyStMgrClientMgr() { }

  virtual void CloseConnectionsAndClear() { close_connections_called_ = true; }
  bool CloseConnectionsCalled() const { return close_connections_called_; }

  virtual void StartConnections(heron::proto::system::PhysicalPlan const&) {
    start_connections_called_ = true;
  }
  bool StartConnectionsCalled() const { return start_connections_called_; }

  virtual bool AllStMgrClientsRegistered() { return all_stmgrclients_registered_; }
  virtual void SetAllStMgrClientsRegistered(bool val) { all_stmgrclients_registered_ = val; }

 private:
  bool close_connections_called_;
  bool start_connections_called_;
  bool all_stmgrclients_registered_;
};

#endif
