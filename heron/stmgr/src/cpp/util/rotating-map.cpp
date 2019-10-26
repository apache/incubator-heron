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

#include "util/rotating-map.h"
#include <iostream>
#include <list>
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace stmgr {

RotatingMap::RotatingMap(sp_int32 _nbuckets) {
  for (sp_int32 i = 0; i < _nbuckets; ++i) {
    auto head = new std::unordered_map<sp_int64, sp_int64>();
    buckets_.push_back(head);
  }
}

RotatingMap::~RotatingMap() {
  while (!buckets_.empty()) {
    auto m = buckets_.front();
    buckets_.pop_front();
    delete m;
  }
}

void RotatingMap::rotate() {
  auto m = buckets_.back();
  buckets_.pop_back();
  delete m;
  buckets_.push_front(new std::unordered_map<sp_int64, sp_int64>());
}

void RotatingMap::create(sp_int64 _key, sp_int64 _value) {
  std::unordered_map<sp_int64, sp_int64>* m = buckets_.front();
  (*m)[_key] = _value;
}

bool RotatingMap::anchor(sp_int64 _key, sp_int64 _value) {
  for (auto iter = buckets_.begin(); iter != buckets_.end(); ++iter) {
    std::unordered_map<sp_int64, sp_int64>* m = *iter;
    if (m->find(_key) != m->end()) {
      sp_int64 current_value = (*m)[_key];
      sp_int64 new_value = current_value ^ _value;
      (*m)[_key] = new_value;
      return new_value == 0;
    }
  }
  return false;
}

bool RotatingMap::remove(sp_int64 _key) {
  for (auto iter = buckets_.begin(); iter != buckets_.end(); ++iter) {
    size_t removed = (*iter)->erase(_key);
    if (removed > 0) return true;
  }
  return false;
}

}  // namespace stmgr
}  // namespace heron
