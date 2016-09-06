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

#ifndef __TMASTER_STATS_INTERFACE_H_
#define __TMASTER_STATS_INTERFACE_H_

#include "network/network.h"
#include "proto/tmaster.pb.h"
#include "basics/basics.h"

namespace heron {
namespace tmaster {

class TMetricsCollector;
class TMaster;

class StatsInterface {
 public:
  StatsInterface(EventLoop* eventLoop, const NetworkOptions& options,
                 TMetricsCollector* _collector, TMaster* tmaster);
  virtual ~StatsInterface();

 private:
  void HandleStatsRequest(IncomingHTTPRequest* _request);
  void HandleUnknownRequest(IncomingHTTPRequest* _request);
  void HandleExceptionRequest(IncomingHTTPRequest* _request);
  void HandleExceptionSummaryRequest(IncomingHTTPRequest* _request);

  HTTPServer* http_server_;  // Our http server
  TMetricsCollector* metrics_collector_;
  TMaster* tmaster_;
};
}  // namespace tmaster
}  // namespace heron

#endif
