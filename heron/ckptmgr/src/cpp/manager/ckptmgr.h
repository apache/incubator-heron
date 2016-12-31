
#ifndef SRC_CPP_SVCS_CKPTMGR_SRC_CKPTMANAGER_CKPTMGR_H_
#define SRC_CPP_SVCS_CKPTMGR_SRC_CKPTMANAGER_CKPTMGR_H_

#include "basics/basics.h"
#include "manager/ckptmgr-server.h"
#include "network/network.h"
#include "proto/messages.h"

namespace heron {
namespace ckptmgr {

class CkptMgrServer;

class CkptMgr {
  public:
    CkptMgr(EventLoop* eventLoop, sp_int32 _myport, const sp_string& _topology_name,
            const sp_string& _topology_id, const sp_string& _ckptmgr_id);
    virtual ~CkptMgr();

    void Init();

    // TODO: add methods

  private:
    void StartCkptmgrServer();

    sp_string topology_name_;
    sp_string topology_id_;
    sp_string ckptmgr_id_;
    sp_int32 ckptmgr_port_;
    CkptMgrServer* server_;

    EventLoop* eventLoop_;
};

} // namespace ckptmgr
} // namespace heron

#endif // SRC_CPP_SVCS_CKPTMGR_SRC_CKPTMANAGER_CKPTMGR_H_
