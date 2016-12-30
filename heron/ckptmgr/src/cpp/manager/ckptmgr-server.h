#ifndef SRC_CPP_SVCS_CKPTMGR_SRC_MANAGER_CKPTMGR_SERVER_H_
#define SRC_CPP_SVCS_CKPTMGR_SRC_MANAGER_CKPTMGR_SERVER_H_

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

    void HandleStMgrHelloRequest(REQID _id, Connection* _conn,
                                   proto::stmgr::StrMgrHelloRequest* _request);
//    void HandleCkptStoreRequest(REQID _id, Connection* _conn,
//                                proto::ckptmgr::CkptStoreRequest* _request);
//    void HandleCkptFetchRequest(REQID _id, Connection* _conn,
//                                proto::ckptmgr::CkptFetchRequest* _request);

    sp_string topology_name_;
    sp_string topology_id_;
    sp_string ckptmgr_id_;
    CkptMgr* ckptmgr_;
    Connection* stmgr_conn_;
};

} // namespace ckptmgr
} // namespace heron

#endif