/*
 * Copyright 2017 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

////////////////////////////////////////////////////////////////////////////////
// Just defines the static member of Mempool class
////////////////////////////////////////////////////////////////////////////////
#include "basics/basics.h"

// TODO(nlu): get the pool size limit from config
MemPool<google::protobuf::Message>* __global_protobuf_pool__ =
                                   new MemPool<google::protobuf::Message>(50 * 1024 * 1024);
std::mutex __global_protobuf_pool_mutex__;
