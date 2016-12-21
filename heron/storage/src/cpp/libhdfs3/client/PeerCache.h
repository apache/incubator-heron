/********************************************************************
 * 2014 -
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef _HDFS_LIBHDFS3_CLIENT_PEERCACHE_H_
#define _HDFS_LIBHDFS3_CLIENT_PEERCACHE_H_

#include <string>
#include <utility>

#include "common/DateTime.h"
#include "common/LruMap.h"
#include "common/Memory.h"
#include "common/SessionConfig.h"
#include "network/Socket.h"
#include "server/DatanodeInfo.h"

namespace Hdfs {
namespace Internal {

class PeerCache {
 public:
  explicit PeerCache(const SessionConfig& conf);

  shared_ptr<Socket> getConnection(const DatanodeInfo& datanode);

  void addConnection(shared_ptr<Socket> peer, const DatanodeInfo& datanode);

  typedef std::pair<shared_ptr<Socket>, steady_clock::time_point> value_type;

 private:
  std::string buildKey(const DatanodeInfo& datanode);

 private:
  const int cacheSize;
  int64_t expireTimeInterval;  // milliseconds
  static LruMap<std::string, value_type> Map;
};
}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_PEERCACHE_H_ */
