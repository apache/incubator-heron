
#include "manager/ckptmgr-client.h"
#include <iostream>
#include <string>
#include "basics/basics.h"
#include "proto/messages.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/heron-internals-config-reader.h"

namespace heron {
namespace ckptmgr {

CkptMgrClient::CkptMgrClient(EventLoop* eventloop, const NetworkOptions& _options,
                             const sp_string& _topology_name, const sp_string& _topology_id,
                             const sp_string& _ckptmgr_id, const sp_string& _stmgr_id)
    : Client(eventloop, _options),
      topology_name_(_topology_name),
      topology_id_(_topology_id),
      ckptmgr_id_(_ckptmgr_id),
      stmgr_id_(_stmgr_id),
      quit_(false) {

  reconnect_cpktmgr_interval_sec_ =
    config::HeronInternalsConfigReader::Instance()->GetHeronStreammgrClientReconnectIntervalSec();

  InstallResponseHandler(new proto::stmgr::StrMgrHelloRequest(), &CkptMgrClient::HandleHelloResponse);
}

CkptMgrClient::~CkptMgrClient() {
  Stop();
}


void CkptMgrClient::Quit() {
  quit_ = true;
  Stop();
}

void CkptMgrClient::HandleConnect(NetworkErrorCode _status) {
  if (_status == OK) {
    LOG(INFO) << "Connected to ckptmgr " << ckptmgr_id_ << " running at"
              << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
              << std::endl;
    if (quit_) {
      Stop();
    } else {
      SendHelloRequest();
    }
  } else {
    LOG(WARNING) << "Could not connect to cpktmgr" << " running at "
                 << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
                 << " due to: " << _status << std::endl;
    if (quit_) {
      LOG(ERROR) << "Quitting";
      delete this;
      return ;
    } else {
      LOG(INFO) << "Retrying again..." << std::endl;
      AddTimer([this]() { this->OnReconnectTimer(); },
               reconnect_cpktmgr_interval_sec_ * 1000 * 1000);
    }
  }
}

void CkptMgrClient::HandleClose(NetworkErrorCode _code) {
  if (_code == OK) {
    LOG(INFO) << "We closed our server connection with cpktmgr " << ckptmgr_id_ << "running at "
              << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
              << std::endl;
  } else {
    LOG(INFO) << "Ckptmgr" << ckptmgr_id_ << " running at "
              << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
              << " closed connection with code " << _code << std::endl;
  }
  if (quit_) {
    delete this;
  } else {
    LOG(INFO) << "Will try to reconnect again..." << std::endl;
    AddTimer([this]() { this->OnReconnectTimer(); },
             reconnect_cpktmgr_interval_sec_ * 1000 * 1000);
  }
}

void CkptMgrClient::HandleHelloResponse(void*, proto::stmgr::StrMgrHelloResponse* _response,
                                        NetworkErrorCode _status) {
  if (_status != OK) {
    LOG(ERROR) << "NonOk network code " << _status << " for register response from ckptmgr "
               << ckptmgr_id_ << "running at " << get_clientoptions().get_host() << ":"
               << get_clientoptions().get_port();
    delete _response;
    Stop();
    return;
  }
  proto::system::StatusCode status = _response->status().status();
  if (status != proto::system::OK) {
    LOG(ERROR) << "NonOK register response " << status << " from ckptmgr " << ckptmgr_id_
               << " running at " << get_clientoptions().get_host() << ":"
               << get_clientoptions().get_port();
    Stop();
  } else {
    LOG(INFO) << "Hello request got response OK from ckptmgr" << ckptmgr_id_
              << " running at " << get_clientoptions().get_host() << ":"
              << get_clientoptions().get_port();
  }
  delete _response;
}

void CkptMgrClient::OnReconnectTimer() { Start(); }

void CkptMgrClient::SendHelloRequest() {
  auto request = new proto::stmgr::StrMgrHelloRequest();
  request->set_topology_name(topology_name_);
  request->set_topology_id(topology_id_);
  request->set_stmgr(stmgr_id_);
  SendRequest(request, NULL);
  return ;
}


}  // namespace ckptmgr
}  // namespace heorn