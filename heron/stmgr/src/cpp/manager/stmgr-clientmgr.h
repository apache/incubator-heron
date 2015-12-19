#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_CLIENTMGR_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_CLIENTMGR_H_

#include <map>

namespace heron {
namespace common {
class MetricsMgrSt;
class MultiCountMetric;
}
}

namespace heron {
namespace stmgr {
class StMgr;
class StMgrClient;

class StMgrClientMgr {
 public:
  StMgrClientMgr(EventLoop* eventLoop, const sp_string& _topology_name,
                 const sp_string& _topology_id, const sp_string& _stmgr_id,
                 StMgr* _stream_manager,
                 heron::common::MetricsMgrSt* _metrics_manager_client);
  virtual ~StMgrClientMgr();

  void NewPhysicalPlan(const proto::system::PhysicalPlan* _pplan);
  void SendTupleStreamMessage(const sp_string& _stmgr_id,
                              proto::stmgr::TupleStreamMessage* _msg);

  // Forward the call to the stmgr
  void StartBackPressureOnServer(const sp_string& _other_stmgr_id);
  // Forward the call to the stmgr
  void StopBackPressureOnServer(const sp_string& _other_stmgr_id);
  // Used by the server to tell the client to send the back pressure related
  // messages
  void SendStartBackPressureToOtherStMgrs();
  void SendStopBackPressureToOtherStMgrs();
  bool DidAnnounceBackPressure();

 private:
  StMgrClient* CreateClient(const sp_string& _other_stmgr_id,
                            const sp_string& _host_name, sp_int32 _port);

  // map of stmgrid to its client
  std::map<sp_string, StMgrClient*> clients_;

  sp_string topology_name_;
  sp_string topology_id_;
  sp_string stmgr_id_;
  EventLoop* eventLoop_;

  StMgr* stream_manager_;
  // Metrics
  heron::common::MetricsMgrSt* metrics_manager_client_;
  heron::common::MultiCountMetric* stmgr_clientmgr_metrics_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STMGR_CLIENTMGR_H_
