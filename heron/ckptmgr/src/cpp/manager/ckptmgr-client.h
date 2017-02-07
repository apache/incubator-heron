

#ifndef SRC_CPP_SVCS_CKPTMGR_SRC_MANAGER_CKPTMGR_CLIENT_H_
#define SRC_CPP_SVCS_CKPTMGR_SRC_MANAGER_CKPTMGR_CLIENT_H_

#include "basics/basics.h"
#include "network/network.h"
#include "network/network_error.h"
#include "proto/messages.h"

namespace heron {
namespace ckptmgr {

class CkptMgrClient : public Client {
  public:
    CkptMgrClient(EventLoop* eventLoop, const NetworkOptions& _options,
                  const sp_string& _topology_name, const sp_string& _topology_id,
                  const sp_string& _our_ckptmgr_id, const sp_string& _our_stmgr_id);
    virtual ~CkptMgrClient();

    void Quit();

    // TODO: add actual requests
    //void StoreCheckPoint(CheckPoint cp);
    //CheckPoint FetchCheckPoint();

  protected:
    virtual void HandleConnect(NetworkErrorCode status);
    virtual void HandleClose(NetworkErrorCode status);

  private:
    void HandleHelloResponse(void *, proto::stmgr::StrMgrHelloResponse *_response, NetworkErrorCode);
    void SendHelloRequest();

    void OnReconnectTimer();

    // TODO: add actual response handler methods

    sp_string topology_name_;
    sp_string topology_id_;
    sp_string ckptmgr_id_;
    sp_string stmgr_id_;
    bool quit_;

    // Config
    sp_int32 reconnect_cpktmgr_interval_sec_;
};

} // namespace ckptmgr
} // namespace heron

#endif  // SRC_CPP_SVCS_CKPTMGR_SRC_MANAGER_CKPTMGR_CLIENT_H_