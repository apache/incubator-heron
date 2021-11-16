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

#ifndef HERON_INSTANCE_EXECUTOR_INSTANCE_BASE_H_
#define HERON_INSTANCE_EXECUTOR_INSTANCE_BASE_H_

#include <string>
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

namespace heron {
namespace instance {

class InstanceBase {
 public:
  virtual ~InstanceBase() { }
  virtual void Start() = 0;
  virtual void Activate() = 0;
  virtual void Deactivate() = 0;
  virtual bool IsRunning() = 0;
  virtual void DoWork() = 0;
  virtual void HandleGatewayTuples(pool_unique_ptr<proto::system::HeronTupleSet2> tupleSet) = 0;
};

}  // namespace instance
}  // namespace heron

#endif  // HERON_INSTANCE_EXECUTOR_INSTANCE_BASE_H_
