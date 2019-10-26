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

////////////////////////////////////////////////////////////////////////////////
// Define some hash functions for hash_set/hash_map and stuff
///////////////////////////////////////////////////////////////////////////////

#ifndef __SP_HASH_H
#define __SP_HASH_H

#include <functional>
#include <string>
#include <utility>
#include "basics/ridgen.h"

namespace std {

template <>
struct hash<std::pair<std::string, std::string> > {
  std::size_t operator()(const pair<std::string, std::string> &key) const {
    return std::hash<std::string>()(key.first + key.second);
  }
};

template <>
struct hash<std::pair<std::string, sp_int32> > {
  std::size_t operator()(const pair<std::string, sp_int32> &key) const {
    return std::hash<std::string>()(key.first) ^ std::hash<sp_int32>()(key.second);
  }
};

template <>
struct hash<REQID> {
  size_t operator()(const REQID &key) const { return std::hash<std::string>()(key.str()); }
};

}  // namespace std

#endif  // __HASHFN_H
