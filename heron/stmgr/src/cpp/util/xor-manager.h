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

#ifndef SRC_CPP_SVCS_STMGR_SRC_UTIL_XOR_MANAGER_H_
#define SRC_CPP_SVCS_STMGR_SRC_UTIL_XOR_MANAGER_H_

#include <unordered_map>
#include <vector>
#include "proto/messages.h"
#include "basics/basics.h"
#include "network/network.h"

namespace heron {
namespace stmgr {

class RotatingMap;

class XorManager {
 public:
  XorManager(std::shared_ptr<EventLoop> eventLoop, sp_int32 _timeout,
          const std::vector<sp_int32>& _task_ids);
  virtual ~XorManager();

  // Create a new entry for the tuple.
  // _task_id is the task id where the tuple
  // originated from.
  // _key is the tuple key
  // _value is the tuple key as seen by the
  // destination
  void create(sp_int32 _task_id, sp_int64 _key, sp_int64 _value);

  // Add one more entry to the tuple tree
  // _task_id is the task id where the tuple
  // originated from.
  // _key is the tuple key
  // _value is the tuple key as seen by the
  // destination
  // We return true if the xor value is now zerod out
  // Else return false
  bool anchor(sp_int32 _task_id, sp_int64 _key, sp_int64 _value);

  // remove this tuple key from our structure.
  // return true if this key was found. else false
  bool remove(sp_int32 _task_id, sp_int64 _key);

 private:
  void rotate(EventLoopImpl::Status _status);

  std::shared_ptr<EventLoop> eventLoop_;
  sp_int32 timeout_;

  // map of task_id to a RotatingMap
  std::unordered_map<sp_int32, unique_ptr<RotatingMap>> tasks_;

  // Configs to be read
  sp_int32 n_buckets_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_UTIL_XOR_MANAGER_H_
