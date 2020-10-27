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

#ifndef __TMANAGER_STATEFUL_RESTORER_H_
#define __TMANAGER_STATEFUL_RESTORER_H_

#include <set>
#include <string>
#include "manager/tmanager.h"

namespace heron {
namespace tmanager {

class StatefulRestorer {
 public:
  StatefulRestorer();
  virtual ~StatefulRestorer();
  // Start a new 2 Phase Commit with this checkpoint_id
  void StartRestore(const std::string& _ckeckpoint_id,
                    const StMgrMap& _stmgrs);
  void HandleStMgrRestored(const std::string& _stmgr_id,
                      const std::string& _ckeckpoint_id,
                      int64_t _restore_txid,
                      const StMgrMap& _stmgrs);

  bool GotResponse(const std::string& _stmgr) const;

  // accessor functions
  bool IsInProgress() const { return in_progress_; }
  int64_t GetRestoreTxid() const { return restore_txid_; }
  const std::string& GetCheckpointIdInProgress() const { return checkpoint_id_in_progress_; }

 private:
  void Finish2PhaseCommit(const StMgrMap& _stmgrs);
  bool in_progress_;
  int64_t restore_txid_;
  std::string checkpoint_id_in_progress_;
  std::set<std::string> unreplied_stmgrs_;
};
}  // namespace tmanager
}  // namespace heron

#endif
