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

#ifndef SRC_CPP_SVCS_CKPTMGR_SRC_CKPTMANAGER_CKPTMGR_SERVER_H_
#define SRC_CPP_SVCS_CKPTMGR_SRC_CKPTMANAGER_CKPTMGR_SERVER_H_

#include "basics/basics.h"
#include "manager/ckptmgr.h"
#include "manager/ckptmgr-server.h"
#include "network/network.h"
#include "proto/messages.h"

namespace heron {
namespace ckptmgr {

class CkptMgr;

class CkptMgrServer : public Server {
  public:
    CkptMgrServer(EventLoop* eventLoop, const NetworkOptions& options,
                  const sp_string& _topology_name, const sp_string& _topology_id,
                  const sp_string& _ckptmgr_id, CkptMgr* _ckptmgr);
    virtual ~CkptMgrServer();

  protected:
    virtual void HandleNewConnection(Connection* newConnection);
    virtual void HandleConnectionClose(Connection* connection, NetworkErrorCode status);

  private:

    void HandleStMgrRegisterRequest(REQID _id, Connection* _conn,
                                   proto::ckptmgr::RegisterStMgrRequest* _request);

    sp_string topology_name_;
    sp_string topology_id_;
    sp_string ckptmgr_id_;
    CkptMgr* ckptmgr_;
    Connection* stmgr_conn_;
};

} // namespace ckptmgr
} // namespace heron

#endif // SRC_CPP_SVCS_CKPTMGR_SRC_CKPTMANAGER_CKPTMGR_SERVER_H_
