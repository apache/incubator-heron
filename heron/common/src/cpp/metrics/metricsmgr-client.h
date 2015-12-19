#ifndef __METRICSMGR_CLIENT_H_
#define __METRICSMGR_CLIENT_H_

#include "network/network_error.h"

namespace heron {
  namespace proto {
    namespace tmaster {
      class TMasterLocation;
    }
  }
}

namespace heron { namespace common {

class MetricsMgrClient : public Client
{
 public:
  MetricsMgrClient(const sp_string& _hostname,
                   sp_int32 _port,
                   const sp_string& _component_id,
                   const sp_string& _task_id,
                   EventLoop* eventLoop,
                   const NetworkOptions& options);
  ~MetricsMgrClient();

  void SendMetrics(proto::system::MetricPublisherPublishMessage* _message);
  void SendTMasterLocation(const proto::tmaster::TMasterLocation& location);

 protected:
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

 private:
  void InternalSendTMasterLocation();
  void ReConnect();
  void SendRegisterRequest();
  void HandleRegisterResponse(void* _ctx,
               proto::system::MetricPublisherRegisterResponse* _respose,
               NetworkErrorCode _status);

  sp_string                                hostname_;
  sp_int32                                 port_;
  sp_string                                component_id_;
  sp_string                                task_id_;
  proto::tmaster::TMasterLocation* tmaster_location_;
  // Tells if we have registered to metrics manager or not
  bool                                     registered_;
};

}} // end namespace

#endif
