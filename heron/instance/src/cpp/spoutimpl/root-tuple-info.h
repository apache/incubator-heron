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

#ifndef HERON_INSTANCE_SPOUT_ROOT_TUPLE_INFO_H_
#define HERON_INSTANCE_SPOUT_ROOT_TUPLE_INFO_H_

#include <string>
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

namespace heron {
namespace instance {

class RootTupleInfo {
 public:
  RootTupleInfo(const std::string& streamId, int64_t messageId)
    : streamId_(streamId), messageId_(messageId) {
    insertionTimeNs_ = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::system_clock::now().time_since_epoch()).count();
  }
  ~RootTupleInfo() { }

  const std::string& getStreamId() const { return streamId_; }
  int64_t getMessageId() const { return messageId_; }
  int64_t getInsertionTime() const { return insertionTimeNs_; }
  bool isExpired(int64_t currentTimeNs, int64_t timeout) {
    return currentTimeNs - insertionTimeNs_ > timeout;
  }

  bool operator == (const RootTupleInfo& other) const {
    return other.getInsertionTime() == insertionTimeNs_;
  }

  bool operator < (const RootTupleInfo& other) const {
    return insertionTimeNs_ < other.getInsertionTime();
  }

 private:
  std::string streamId_;
  int64_t messageId_;
  int64_t insertionTimeNs_;
};

}  // namespace instance
}  // namespace heron

#endif  // HERON_INSTANCE_SPOUT_ROOT_TUPLE_INFO_H_
