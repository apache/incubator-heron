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

#ifndef TMASTER_PROCESSOR_H_
#define TMASTER_PROCESSOR_H_

#include "proto/messages.h"
#include "basics/basics.h"
#include "network/network.h"

namespace heron {
namespace tmaster {

class TMaster;

class Processor {
 public:
  Processor(REQID _reqid, Connection* _conn, google::protobuf::Message* _request, TMaster* _tmaster,
            Server* _server);
  virtual ~Processor();
  virtual void Start() = 0;

 protected:
  void SendResponse(const google::protobuf::Message& _response);
  Connection* GetConnection() { return conn_; }
  void CloseConnection();
  google::protobuf::Message* request_;
  TMaster* tmaster_;
  Server* server_;

 private:
  REQID reqid_;
  Connection* conn_;
};
}  // namespace tmaster
}  // namespace heron
#endif
