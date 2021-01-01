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

#ifndef __DUMMY_CKPTMGR_CLIENT_H
#define __DUMMY_CKPTMGR_CLIENT_H

#include <map>
#include <set>
#include <string>

#include "proto/messages.h"
#include "manager/ckptmgr-client.h"

class DummyCkptMgrClient : public heron::stmgr::CkptMgrClient {
 public:
  DummyCkptMgrClient(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& _options,
                     const std::string& _stmgr,
                     heron::proto::system::PhysicalPlan* _pplan);

  virtual ~DummyCkptMgrClient();

  virtual void SaveInstanceState(unique_ptr<heron::proto::ckptmgr::SaveInstanceStateRequest> _request);
  virtual void GetInstanceState(const heron::proto::system::Instance& _instance,
                                const std::string& _checkpoint_id);

  bool SaveCalled(const std::string& _ckpt_id, int32_t _task_id) const;
  bool GetCalled(const std::string& _ckpt_id, int32_t _task_id) const;
  void ClearGetCalled(const std::string& _ckpt_id, int32_t _task_id);

 private:
  void DummySaveWatcher(const heron::proto::system::Instance& _instance,
                        const std::string& _ckpt_id);
  void DummyGetWatcher(heron::proto::system::StatusCode _status,
                       int32_t _restore_txid,
                       const std::string& _ckpt_id,
                       const heron::proto::ckptmgr::InstanceStateCheckpoint& _ckpt);
  void DummyRegisterWatcher();
  // Map from ckptid to task_ids for which the call was made
  std::map<std::string, std::set<int32_t>> saves_;
  std::map<std::string, std::set<int32_t>> gets_;
};

#endif
