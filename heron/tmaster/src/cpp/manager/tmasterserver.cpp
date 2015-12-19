#include <iostream>

#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "metrics/tmaster-metrics.h"
#include "processor/processor.h"
#include "manager/tmetrics-collector.h"
#include "manager/tmaster.h"
#include "manager/tmasterserver.h"

namespace heron { namespace tmaster {

TMasterServer::TMasterServer(EventLoop* eventLoop,
                             const NetworkOptions& _options,
                             TMetricsCollector* _collector,
                             TMaster* _tmaster)
  : Server(eventLoop, _options), collector_(_collector), tmaster_(_tmaster)
{
  // Install the stmgr handlers
  InstallRequestHandler(&TMasterServer::HandleStMgrRegisterRequest);
  InstallRequestHandler(&TMasterServer::HandleStMgrHeartbeatRequest);

  // Install the metricsmgr handlers
  InstallMessageHandler(&TMasterServer::HandleMetricsMgrStats);
}

TMasterServer::~TMasterServer()
{
  // Nothing really
}

void TMasterServer::HandleNewConnection(Connection*)
{
  // There is nothing to be done here. Instead we wait for
  // the register message
}

void TMasterServer::HandleConnectionClose(Connection* _conn,
                                          NetworkErrorCode)
{
  if (tmaster_->RemoveStMgrConnection(_conn) != proto::system::OK) {
    LOG(WARNING) << "Unknown connection closed on us from "
               << _conn->getIPAddress() << ":"
               << _conn->getPort()
               << ", possibly metrics mgr";
    return;
  }
}

void TMasterServer::HandleStMgrRegisterRequest(REQID _reqid,
                                               Connection* _conn,
                                               proto::tmaster::StMgrRegisterRequest* _request)
{
  StMgrRegisterProcessor* processor =
        new StMgrRegisterProcessor(_reqid, _conn, _request,
                                   tmaster_, this);
  processor->Start();
}

void TMasterServer::HandleStMgrHeartbeatRequest(REQID _reqid,
                                                Connection* _conn,
                                                proto::tmaster::StMgrHeartbeatRequest* _request)
{
  StMgrHeartbeatProcessor* processor =
                       new StMgrHeartbeatProcessor(_reqid, _conn,
                                                   _request, tmaster_, this);
  processor->Start();
}

void TMasterServer::HandleMetricsMgrStats(Connection*, proto::tmaster::PublishMetrics* _request)
{
  collector_->AddMetric(*_request);
  delete _request;
}

}} // end of namespace
