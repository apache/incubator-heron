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

#ifndef SRC_CPP_SVCS_CKPTMGR_SRC_CKPTMANAGER_CKPTMGR_H_
#define SRC_CPP_SVCS_CKPTMGR_SRC_CKPTMANAGER_CKPTMGR_H_

#include "basics/basics.h"
#include "network/network.h"
#include "proto/messages.h"
#include "common/checkpoint.h"
#include "common/storage.h"

namespace heron {
namespace ckptmgr {

class CkptMgrServer;

class CkptMgr {
 public:
  CkptMgr(EventLoop* eventLoop, sp_int32 _myport, const sp_string& _topology_name,
          const sp_string& _topology_id, const sp_string& _ckptmgr_id,
          Storage* _storage);
  virtual ~CkptMgr();

  void Init();

  // TODO(nlu): add methods

  // get the storage
  Storage* storage() {
    return storage_;
  }

 private:
  void StartCkptmgrServer();

  sp_string topology_name_;
  sp_string topology_id_;
  sp_string ckptmgr_id_;
  sp_int32 ckptmgr_port_;

  Storage* storage_;

  CkptMgrServer* server_;
  EventLoop* eventLoop_;
};

}  // namespace ckptmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_CKPTMGR_SRC_CKPTMANAGER_CKPTMGR_H_
