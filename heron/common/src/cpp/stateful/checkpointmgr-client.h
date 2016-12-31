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

#ifndef __CHECKPOINTMGR_CLIENT_H_
#define __CHECKPOINTMGR_CLIENT_H_

#include "network/network_error.h"
#include "network/network.h"
#include "proto/messages.h"
#include "basics/basics.h"

namespace heron {
namespace proto {
namespace ckptmgr {
class SaveStateCheckpoint;
}
}
}

namespace heron {
namespace common {

class CheckpointMgrClient : public Client {
 public:
  CheckpointMgrClient(EventLoop* eventLoop, const NetworkOptions& options);
  ~CheckpointMgrClient();

  void SaveStateCheckpoint(proto::ckptmgr::SaveStateCheckpoint* _message);

 protected:
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

 private:
  void ReConnect();
};
}  // namespace common
}  // namespace heron

#endif
