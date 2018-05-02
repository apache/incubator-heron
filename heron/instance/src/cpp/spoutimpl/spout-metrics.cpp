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

#include <dlfcn.h>
#include <stdlib.h>

#include <list>
#include <string>

#include "spoutimpl/spout-metrics.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"
#include "config/heron-internals-config-reader.h"

namespace heron {
namespace instance {

SpoutMetrics::SpoutMetrics(std::shared_ptr<api::metric::IMetricsRegistrar> metricsRegistrar) {
  ackCount_.reset(new api::metric::MultiCountMetric());
  failCount_.reset(new api::metric::MultiCountMetric());
  timeoutCount_.reset(new api::metric::MultiCountMetric());
  emitCount_.reset(new api::metric::MultiCountMetric());
  nextTupleCount_.reset(new api::metric::CountMetric());
  completeLatency_.reset(new api::metric::MultiMeanMetric());
  failLatency_.reset(new api::metric::MultiMeanMetric());
  nextTupleLatency_.reset(new api::metric::MeanMetric());

  int interval = config::HeronInternalsConfigReader::Instance()
                 ->GetHeronMetricsExportIntervalSec();
  metricsRegistrar->registerMetric("__ack-count", ackCount_, interval);
  metricsRegistrar->registerMetric("__fail-count", failCount_, interval);
  metricsRegistrar->registerMetric("__timeout-count", timeoutCount_, interval);
  metricsRegistrar->registerMetric("__emit-count", emitCount_, interval);
  metricsRegistrar->registerMetric("__next-tuple-count", nextTupleCount_, interval);
  metricsRegistrar->registerMetric("__complete-latency", completeLatency_, interval);
  metricsRegistrar->registerMetric("__fail-latency", failLatency_, interval);
  metricsRegistrar->registerMetric("__next-tuple-latency", nextTupleLatency_, interval);
}

SpoutMetrics::~SpoutMetrics() {
}

void SpoutMetrics::ackedTuple(const std::string& streamId, int64_t latency) {
  ackCount_->scope(streamId)->incr();
  completeLatency_->scope(streamId)->incr(latency);
}

void SpoutMetrics::failedTuple(const std::string& streamId, int64_t latency) {
  failCount_->scope(streamId)->incr();
  failLatency_->scope(streamId)->incr(latency);
}

void SpoutMetrics::timeoutTuple(const std::string& streamId) {
  timeoutCount_->scope(streamId)->incr();
}

void SpoutMetrics::emittedTuple(const std::string& streamId) {
  emitCount_->scope(streamId)->incr();
}

void SpoutMetrics::nextTuple(int64_t latency) {
  nextTupleCount_->incr();
  nextTupleLatency_->incr(latency);
}

}  // namespace instance
}  // namespace heron
