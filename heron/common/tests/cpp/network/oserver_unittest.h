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

#ifndef _ORDER_SERVER_H
#define _ORDER_SERVER_H

#include <map>
#include "network/unittests.pb.h"
#include "network/network_error.h"
#include "network/network.h"
#include "basics/basics.h"

class OrderServer : public Server {
 public:
  OrderServer(std::shared_ptr<EventLoopImpl> ss, const NetworkOptions& options);

  ~OrderServer();

  // total packets recvd
  sp_uint64 recv_pkts() { return nrecv_; }

  // total packets sent
  sp_uint64 sent_pkts() { return nsent_; }

 protected:
  // handle an incoming connection from server
  virtual void HandleNewConnection(Connection* newConnection);

  // handle a connection close
  virtual void HandleConnectionClose(Connection* connection, NetworkErrorCode status);

  // handle the test message
  virtual void HandleOrderMessage(Connection* connection, pool_unique_ptr<OrderMessage> message);

  // handle the terminate message
  virtual void HandleTerminateMessage(Connection* connection,
          pool_unique_ptr<TerminateMessage> message);

 private:
  void Terminate();

  class msgid {
   public:
    msgid() : ids_(0), idr_(0) {}
    ~msgid() {}

    sp_uint64 incr_ids() {
      sp_uint64 t = ids_++;
      return t;
    }

    sp_uint64 incr_idr() {
      sp_uint64 t = idr_++;
      return t;
    }

   private:
    sp_uint64 ids_;
    sp_uint64 idr_;
  };

  std::map<Connection*, msgid*> clients_;

  sp_uint64 nrecv_;
  sp_uint64 nsent_;
};

#endif
