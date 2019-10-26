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

#ifndef HERON_API_SPOUT_ISPOUT_OUTPUT_COLLECTOR_H_
#define HERON_API_SPOUT_ISPOUT_OUTPUT_COLLECTOR_H_

#include <string>
#include <tuple>
#include <vector>

#include "serializer/ipluggable-serializer.h"
#include "serializer/tuple-serializer-utils.h"
#include "utils/utils.h"

namespace heron {
namespace api {
namespace spout {

class ISpoutOutputCollector {
 public:
  explicit ISpoutOutputCollector(std::shared_ptr<serializer::IPluggableSerializer> serializer)
    : serializer_(serializer) { }

  template<typename ... Args>
  void emit(std::tuple<Args...>& tup) {
    emit(utils::Utils::DEFAULT_STREAM_ID, tup);
  }

  template<typename ... Args>
  void emit(std::tuple<Args...>& tup, int64_t msgId) {
    emit(utils::Utils::DEFAULT_STREAM_ID, tup, msgId);
  }

  template<typename ... Args>
  void emit(const std::string& streamId, std::tuple<Args...>& tup) {
    emit(streamId, tup, -1);
  }

  template<typename ... Args>
  void emit(const std::string& streamId, std::tuple<Args...>& tup, int64_t msgId) {
    std::vector<std::string> serialized_values;
    serializer::TupleSerializerHelper::serialize_tuple(serialized_values,
                                                       serializer_, tup);
    emitInternal(streamId, serialized_values, msgId);
  }

  virtual void reportError(std::exception& except) = 0;

 protected:
  virtual void emitInternal(const std::string& streamid,
                            const std::vector<std::string>& tup,
                            int64_t msgId) = 0;
  std::shared_ptr<serializer::IPluggableSerializer> serializer_;
};

}  // namespace spout
}  // namespace api
}  // namespace heron

#endif  // HERON_API_SPOUT_ISPOUT_OUTPUT_COLLECTOR_H_
