/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef __TMANAGER_STATS_INTERFACE_H_
#define __TMANAGER_STATS_INTERFACE_H_

#include "network/network.h"
#include "proto/tmanager.pb.h"
#include "basics/basics.h"

namespace heron {
namespace tmanager {

using std::unique_ptr;
using std::shared_ptr;

class TMetricsCollector;
class TManager;

class StatsInterface {
 public:
  StatsInterface(std::shared_ptr<EventLoop> eventLoop, const NetworkOptions& options,
                 shared_ptr<TMetricsCollector> _collector, TManager* tmanager);
  virtual ~StatsInterface();

 private:
  void HandleStatsRequest(IncomingHTTPRequest* _request);
  void HandleUnknownRequest(IncomingHTTPRequest* _request);
  void HandleExceptionRequest(IncomingHTTPRequest* _request);
  void HandleExceptionSummaryRequest(IncomingHTTPRequest* _request);
  void HandleStmgrsRegistrationSummaryRequest(IncomingHTTPRequest* _request);

  unique_ptr<HTTPServer> http_server_;  // Our http server
  shared_ptr<TMetricsCollector> metrics_collector_;
  TManager* tmanager_;
};
}  // namespace tmanager
}  // namespace heron

#endif
