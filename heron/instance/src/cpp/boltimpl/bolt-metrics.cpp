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

#include <list>
#include <string>

#include "boltimpl/bolt-metrics.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"
#include "config/heron-internals-config-reader.h"

namespace heron {
namespace instance {

BoltMetrics::BoltMetrics(std::shared_ptr<api::metric::IMetricsRegistrar> metricsRegistrar) {
  ackCount_.reset(new api::metric::MultiCountMetric());
  failCount_.reset(new api::metric::MultiCountMetric());
  executeCount_.reset(new api::metric::MultiCountMetric());
  processLatency_.reset(new api::metric::MultiMeanMetric());
  executeLatency_.reset(new api::metric::MultiMeanMetric());
  failLatency_.reset(new api::metric::MultiMeanMetric());
  executeTime_.reset(new api::metric::MultiCountMetric());
  emitCount_.reset(new api::metric::MultiCountMetric());

  int interval = config::HeronInternalsConfigReader::Instance()
                 ->GetHeronMetricsExportIntervalSec();
  metricsRegistrar->registerMetric("__ack-count", ackCount_, interval);
  metricsRegistrar->registerMetric("__fail-count", failCount_, interval);
  metricsRegistrar->registerMetric("__execute-count", executeCount_, interval);
  metricsRegistrar->registerMetric("__process-latency", processLatency_, interval);
  metricsRegistrar->registerMetric("__fail-latency", failLatency_, interval);
  metricsRegistrar->registerMetric("__execute-latency", executeLatency_, interval);
  metricsRegistrar->registerMetric("__execute-time-ns", executeTime_, interval);
  metricsRegistrar->registerMetric("__emit-count", emitCount_, interval);
}

BoltMetrics::~BoltMetrics() {
}

void BoltMetrics::ackedTuple(const std::string& streamId, const std::string& source,
                             int64_t latency) {
  ackCount_->scope(streamId)->incr();
  processLatency_->scope(streamId)->incr(latency);

  // Different sources could have the same streamIds. We need to
  // count that as well
  std::string globalStreamId = source + "/" + streamId;
  ackCount_->scope(globalStreamId)->incr();
  processLatency_->scope(globalStreamId)->incr(latency);
}

void BoltMetrics::failedTuple(const std::string& streamId, const std::string& source,
                             int64_t latency) {
  failCount_->scope(streamId)->incr();
  failLatency_->scope(streamId)->incr(latency);

  // Different sources could have the same streamIds. We need to
  // count that as well
  std::string globalStreamId = source + "/" + streamId;
  failCount_->scope(globalStreamId)->incr();
  failLatency_->scope(globalStreamId)->incr(latency);
}

void BoltMetrics::executeTuple(const std::string& streamId, const std::string& source,
                               int64_t latency) {
  executeCount_->scope(streamId)->incr();
  executeLatency_->scope(streamId)->incr(latency);
  executeTime_->scope(streamId)->incrBy(latency);

  // Different sources could have the same streamIds. We need to
  // count that as well
  std::string globalStreamId = source + "/" + streamId;
  executeCount_->scope(globalStreamId)->incr();
  executeLatency_->scope(globalStreamId)->incr(latency);
  executeTime_->scope(globalStreamId)->incrBy(latency);
}

void BoltMetrics::emittedTuple(const std::string& streamId) {
  emitCount_->scope(streamId)->incr();
}

}  // namespace instance
}  // namespace heron
