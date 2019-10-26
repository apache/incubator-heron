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

#ifndef HERON_API_BOLT_BASIC_BOLT_COLLECTOR_H_
#define HERON_API_BOLT_BASIC_BOLT_COLLECTOR_H_

#include <list>
#include <string>
#include <tuple>
#include <vector>
#include <memory>

#include "bolt/ibolt-output-collector.h"
#include "tuple/tuple.h"
#include "serializer/ipluggable-serializer.h"
#include "serializer/tuple-serializer-utils.h"

namespace heron {
namespace api {
namespace bolt {

class BasicBoltCollector {
 public:
  explicit BasicBoltCollector(std::shared_ptr<IBoltOutputCollector> collector)
    : collector_(collector) { }

  void setContext(std::shared_ptr<tuple::Tuple> inputTuple) {
    inputTuple_ = inputTuple;
  }

  template<typename ... Args>
  void emit(std::tuple<Args...> tup) {
    emit(utils::Utils::DEFAULT_STREAM_ID, tup);
  }

  template<typename ... Args>
  void emit(const std::string& streamId, std::tuple<Args...> tup) {
    collector_->emit(streamId, inputTuple_, tup);
  }

  void reportError(std::exception& except) {
    collector_->reportError(except);
  }

  void ack(std::shared_ptr<tuple::Tuple> input) {
    collector_->ack(input);
  }

  void fail(std::shared_ptr<tuple::Tuple> input) {
    collector_->fail(input);
  }

 protected:
  std::shared_ptr<IBoltOutputCollector> collector_;
  std::shared_ptr<tuple::Tuple> inputTuple_;
};

}  // namespace bolt
}  // namespace api
}  // namespace heron

#endif  // HERON_API_BOLT_BASIC_BOLT_COLLECTOR_H_
