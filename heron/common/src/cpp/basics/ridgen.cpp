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

#include "basics/ridgen.h"
#include <sstream>
#include <string>
#include "glog/logging.h"
#include "kashmir/uuid.h"
#include "kashmir/devrandom.h"

void REQID::assign(const std::string& _id) {
  CHECK_EQ(_id.length(), REQID_size) << "ReqID should be " << REQID_size << " chars!";
  id_ = _id;
}

REQID_Generator::REQID_Generator() {
  using kashmir::system::DevRandom;
  rands_ = reinterpret_cast<void*>(new DevRandom);
}

REQID_Generator::~REQID_Generator() {
  using kashmir::system::DevRandom;

  DevRandom* rs = reinterpret_cast<DevRandom*>(rands_);
  delete rs;
}

REQID
REQID_Generator::generate() {
  using kashmir::system::DevRandom;

  // Generate a unique ID
  kashmir::uuid_t uuid;
  DevRandom* rs = reinterpret_cast<DevRandom*>(rands_);
  (*rs) >> uuid;

  // Convert into string
  std::ostringstream ss;
  ss << uuid;
  return REQID(ss.str());
}

REQID
REQID_Generator::generate_zero_reqid() { return REQID(); }
