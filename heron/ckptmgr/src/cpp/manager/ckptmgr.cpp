
#include "manager/ckptmgr.h"
#include <iostream>
#include "manager/ckptmgr-server.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "network/network.h"

namespace heron{
namespace ckptmgr {

CkptMgr::CkptMgr(EventLoop* eventLoop, sp_int32 _myport, const sp_string& _topology_name,
                 const sp_string& _topology_id, const sp_string& _ckptmgr_id)
    : topology_name_(_topology_name),
      topology_id_(_topology_id),
      ckptmgr_id_(_ckptmgr_id),
      eventLoop_(eventLoop),
      server_(NULL) {}

void CkptMgr::Init() {
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

} // namespace ckptmgr
} // namespace heron
