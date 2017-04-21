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

#include "tmaster/src/cpp/manager/stats-interface.h"
#include <iostream>
#include <string>
#include "manager/tmaster.h"
#include "manager/tmetrics-collector.h"
#include "metrics/tmaster-metrics.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "proto/tmaster.pb.h"

namespace heron {
namespace tmaster {

StatsInterface::StatsInterface(EventLoop* eventLoop, const NetworkOptions& _options,
                               TMetricsCollector* _collector, TMaster* _tmaster)
    : metrics_collector_(_collector), tmaster_(_tmaster) {
  http_server_ = new HTTPServer(eventLoop, _options);
  // Install the handlers
  auto cbHandleStats = [this](IncomingHTTPRequest* request) { this->HandleStatsRequest(request); };

  auto cbHandleException = [this](IncomingHTTPRequest* request) {
    this->HandleExceptionRequest(request);
  };

  auto cbHandleExceptionSummary = [this](IncomingHTTPRequest* request) {
    this->HandleExceptionSummaryRequest(request);
  };

  auto cbHandleStmgrsRegistrationSummary = [this](IncomingHTTPRequest* request) {
    this->HandleStmgrsRegistrationSummaryRequest(request);
  };

  auto cbHandleUnknown = [this](IncomingHTTPRequest* request) {
    this->HandleUnknownRequest(request);
  };

  http_server_->InstallCallBack("/stats", std::move(cbHandleStats));
  http_server_->InstallCallBack("/exceptions", std::move(cbHandleException));
  http_server_->InstallCallBack("/exceptionsummary", std::move(cbHandleExceptionSummary));
  http_server_->InstallCallBack("/stmgrsregistrationsummary",
      std::move(cbHandleStmgrsRegistrationSummary));
  http_server_->InstallGenericCallBack(std::move(cbHandleUnknown));
  CHECK(http_server_->Start() == SP_OK);
}

StatsInterface::~StatsInterface() { delete http_server_; }

void StatsInterface::HandleStatsRequest(IncomingHTTPRequest* _request) {
  LOG(INFO) << "Got a stats request " << _request->GetQuery();
  // get the entire stuff
  unsigned char* pb = _request->ExtractFromPostData(0, _request->GetPayloadSize());
  proto::tmaster::MetricRequest req;
  if (!req.ParseFromArray(pb, _request->GetPayloadSize())) {
    LOG(ERROR) << "Unable to deserialize post data specified in StatsRequest";
    http_server_->SendErrorReply(_request, 400);
    delete _request;
    return;
  }
  proto::tmaster::MetricResponse* res =
    metrics_collector_->GetMetrics(req, tmaster_->getInitialTopology());
  sp_string response_string;
  CHECK(res->SerializeToString(&response_string));
  OutgoingHTTPResponse* response = new OutgoingHTTPResponse(_request);
  response->AddHeader("Content-Type", "application/octet-stream");
  response->AddHeader("Content-Length", std::to_string(response_string.size()));
  response->AddResponse(response_string);
  http_server_->SendReply(_request, 200, response);
  delete res;
  delete _request;
  LOG(INFO) << "Done with stats request ";
}

void StatsInterface::HandleExceptionRequest(IncomingHTTPRequest* _request) {
  LOG(INFO) << "Request for exceptions" << _request->GetQuery();
  // Get the Exception request proto.
  unsigned char* request_data = _request->ExtractFromPostData(0, _request->GetPayloadSize());
  heron::proto::tmaster::ExceptionLogRequest exception_request;
  if (!exception_request.ParseFromArray(request_data, _request->GetPayloadSize())) {
    LOG(ERROR) << "Unable to deserialize post data specified in ExceptionRequest" << std::endl;
    http_server_->SendErrorReply(_request, 400);
    delete _request;
    return;
  }
  heron::proto::tmaster::ExceptionLogResponse* exception_response =
      metrics_collector_->GetExceptions(exception_request);
  sp_string response_string;
  CHECK(exception_response->SerializeToString(&response_string));
  OutgoingHTTPResponse* http_response = new OutgoingHTTPResponse(_request);
  http_response->AddHeader("Content-Type", "application/octet-stream");
  http_response->AddHeader("Content-Length", std::to_string(response_string.size()));
  http_response->AddResponse(response_string);
  http_server_->SendReply(_request, 200, http_response);
  delete exception_response;
  delete _request;
  LOG(INFO) << "Returned exceptions response";
}

void StatsInterface::HandleExceptionSummaryRequest(IncomingHTTPRequest* _request) {
  LOG(INFO) << "Request for exception summary " << _request->GetQuery();
  unsigned char* request_data = _request->ExtractFromPostData(0, _request->GetPayloadSize());
  heron::proto::tmaster::ExceptionLogRequest exception_request;
  if (!exception_request.ParseFromArray(request_data, _request->GetPayloadSize())) {
    LOG(ERROR) << "Unable to deserialize post data specified in ExceptionRequest" << std::endl;
    http_server_->SendErrorReply(_request, 400);
    delete _request;
    return;
  }
  heron::proto::tmaster::ExceptionLogResponse* exception_response =
      metrics_collector_->GetExceptionsSummary(exception_request);
  sp_string response_string;
  CHECK(exception_response->SerializeToString(&response_string));
  OutgoingHTTPResponse* http_response = new OutgoingHTTPResponse(_request);
  http_response->AddHeader("Content-Type", "application/octet-stream");
  http_response->AddHeader("Content-Length", std::to_string(response_string.size()));
  http_response->AddResponse(response_string);
  http_server_->SendReply(_request, 200, http_response);
  delete exception_response;
  delete _request;
  LOG(INFO) << "Returned exceptions response";
}

void StatsInterface::HandleStmgrsRegistrationSummaryRequest(IncomingHTTPRequest* _request) {
  LOG(INFO) << "Request for stream managers registration summary " << _request->GetQuery();
  unsigned char* request_data =
    _request->ExtractFromPostData(0, _request->GetPayloadSize());
  heron::proto::tmaster::StmgrsRegistrationSummaryRequest stmgrs_reg_request;
  if (!stmgrs_reg_request.ParseFromArray(request_data, _request->GetPayloadSize())) {
    LOG(ERROR) << "Unable to deserialize post data specified in" <<
      "StmgrsRegistrationSummaryRequest" << std::endl;
    http_server_->SendErrorReply(_request, 400);
    delete _request;
    return;
  }
  auto stmgrs_reg_summary_response = tmaster_->GetStmgrsRegSummary();
  sp_string response_string;
  CHECK(stmgrs_reg_summary_response->SerializeToString(&response_string));
  auto http_response = new OutgoingHTTPResponse(_request);
  http_response->AddHeader("Content-Type", "application/octet-stream");
  http_response->AddHeader("Content-Length", std::to_string(response_string.size()));
  http_response->AddResponse(response_string);
  http_server_->SendReply(_request, 200, http_response);
  delete stmgrs_reg_summary_response;
  delete _request;
  LOG(INFO) << "Returned stream managers registration summary response";
}

void StatsInterface::HandleUnknownRequest(IncomingHTTPRequest* _request) {
  LOG(WARNING) << "Got an unknown request " << _request->GetQuery();
  http_server_->SendErrorReply(_request, 400);
  delete _request;
}
}  // namespace tmaster
}  // namespace heron
