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
// Just defines the static member of Mempool class
////////////////////////////////////////////////////////////////////////////////
#include "basics/mempool.h"
#include "basics/basics.h"
#include "glog/logging.h"

/* The initial value of pool size limit is 512. This value will
   be overridden by value read from config during initialization
   of stream manager. The reason why we do this is to avoid
   circular dependency problem. See https://github.com/apache/incubator-heron/pull/2073
   for details */
MemPool<google::protobuf::Message>* __global_protobuf_pool__ =
                                   new MemPool<google::protobuf::Message>(512);
std::mutex __global_protobuf_pool_mutex__;

void __global_protobuf_pool_set_pool_max_number_of_messages__(sp_int32 _pool_limit) {
  std::lock_guard<std::mutex> guard(__global_protobuf_pool_mutex__);
  LOG(INFO) << "Set max size of each memory pool to " << _pool_limit;
  __global_protobuf_pool__->set_pool_max_number_of_messages(_pool_limit);
}
