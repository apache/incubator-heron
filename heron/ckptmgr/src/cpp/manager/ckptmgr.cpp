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

#include "manager/ckptmgr.h"
#include <iostream>
#include <limits>
#include "manager/ckptmgr-server.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "network/network.h"

namespace heron {
namespace ckptmgr {

CkptMgr::CkptMgr(EventLoop* eventLoop, sp_int32 _myport, const sp_string& _topology_name,
                 const sp_string& _topology_id, const sp_string& _ckptmgr_id, Storage* _storage)
    : topology_name_(_topology_name),
      topology_id_(_topology_id),
      ckptmgr_id_(_ckptmgr_id),
      ckptmgr_port_(_myport),
      storage_(_storage),
      server_(NULL),
      eventLoop_(eventLoop) {}

void CkptMgr::Init() {
  LOG(INFO) << "Init Ckptmgr" << std::endl;
  StartCkptmgrServer();
}

CkptMgr::~CkptMgr() {
  delete server_;
}

void CkptMgr::StartCkptmgrServer() {
  CHECK(!server_);
  LOG(INFO) << "Creating CkptmgrServer" << std::endl;
  NetworkOptions ops;
  ops.set_host(IpUtils::getHostName());
  ops.set_port(ckptmgr_port_);
  ops.set_socket_family(PF_INET);
  ops.set_max_packet_size(std::numeric_limits<sp_uint32>::max() - 1);
  server_ = new CkptMgrServer(eventLoop_, ops, topology_name_, topology_id_, ckptmgr_id_, this);

  // start the server
  CHECK_EQ(server_->Start(), 0);
}

}  // namespace ckptmgr
}  // namespace heron

