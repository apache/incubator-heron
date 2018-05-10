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

#ifndef STMGR_HEARTBEAT_PROCESSOR_
#define STMGR_HEARTBEAT_PROCESSOR_

#include "processor/tmaster-processor.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "network/network.h"

namespace heron {
namespace tmaster {

class StMgrHeartbeatProcessor : public Processor {
 public:
  StMgrHeartbeatProcessor(REQID _reqid, Connection* _conn,
                          proto::tmaster::StMgrHeartbeatRequest* _request, TMaster* _tmaster,
                          Server* _server);
  virtual ~StMgrHeartbeatProcessor();

  void Start();
};
}  // namespace tmaster
}  // namespace heron
#endif
