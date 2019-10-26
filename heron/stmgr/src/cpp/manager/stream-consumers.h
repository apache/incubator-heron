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

#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_STREAM_CONSUMERS_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_STREAM_CONSUMERS_H_

#include <list>
#include <vector>
#include <typeinfo>   // operator typeid
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"
#include "grouping/shuffle-grouping.h"

namespace heron {
namespace stmgr {

class Grouping;
class ShuffleGrouping;

class StreamConsumers {
 public:
  StreamConsumers(const proto::api::InputStream& _is, const proto::api::StreamSchema& _schema,
                  const std::vector<sp_int32>& _task_ids);
  virtual ~StreamConsumers();

  void NewConsumer(const proto::api::InputStream& _is, const proto::api::StreamSchema& _schema,
                   const std::vector<sp_int32>& _task_ids);

  void GetListToSend(const proto::system::HeronDataTuple& _tuple, std::vector<sp_int32>& _return);

  inline bool isShuffleGrouping() {
      ShuffleGrouping* grouping = dynamic_cast<ShuffleGrouping*>(consumers_.front().get());

    if (consumers_.size() == 1 && grouping != nullptr) {
      return true;
    }
    return false;
  }

 private:
  std::list<std::unique_ptr<Grouping>> consumers_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STREAM_CONSUMERS_H_
