#ifndef __TMASTERSERVER_H
#define __TMASTERSERVER_H

#include "network/network_error.h"

namespace heron { namespace tmaster {

class TMaster;
class TMetricsCollector;

class TMasterServer : public Server
{
 public:
  TMasterServer(EventLoop* eventLoop, const NetworkOptions& options,
                TMetricsCollector* _collector, TMaster* _tmaster);
  virtual ~TMasterServer();

 protected:
  virtual void HandleNewConnection(Connection* newConnection);
  virtual void HandleConnectionClose(Connection* connection, NetworkErrorCode status);

 private:
  // Various handlers for different requests
  void HandleStMgrRegisterRequest(REQID _id, Connection* _conn,
                                  proto::tmaster::StMgrRegisterRequest* _request);
  void HandleStMgrHeartbeatRequest(REQID _id, Connection* _conn,
                                   proto::tmaster::StMgrHeartbeatRequest* _request);
  void HandleMetricsMgrStats(Connection*, proto::tmaster::PublishMetrics* _request);

  // our tmaster
  TMetricsCollector*      collector_;
  TMaster*                tmaster_;
};

}} // end namespace

#endif
