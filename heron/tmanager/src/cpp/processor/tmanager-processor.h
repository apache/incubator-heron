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

#ifndef TMANAGER_PROCESSOR_H_
#define TMANAGER_PROCESSOR_H_

#include "proto/messages.h"
#include "basics/basics.h"
#include "network/network.h"

namespace heron {
namespace tmanager {

class TManager;

class Processor {
 public:
  Processor(REQID _reqid, Connection* _conn, pool_unique_ptr<google::protobuf::Message> _request,
            TManager* _tmanager,
            Server* _server);
  virtual ~Processor();
  virtual void Start() = 0;

 protected:
  void SendResponse(const google::protobuf::Message& _response);
  Connection* GetConnection() { return conn_; }
  void CloseConnection();
  pool_unique_ptr<google::protobuf::Message> request_;
  TManager* tmanager_;
  Server* server_;

 private:
  REQID reqid_;
  Connection* conn_;
};
}  // namespace tmanager
}  // namespace heron
#endif
