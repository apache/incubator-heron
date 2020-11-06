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

#ifndef __DUMMY_MTMGR_H
#define __DUMMY_MTMGR_H

#include "threads/spcountdownlatch.h"
#include "network/network_error.h"

namespace heron {
namespace proto {
namespace tmanager {
class TManagerLocation;
}
}
}

class DummyMtrMgr : public Server {
 public:
  DummyMtrMgr(std::shared_ptr<EventLoopImpl> ss, const NetworkOptions& options,
              const sp_string& stmgr_id,
              CountDownLatch* tmanagerLatch, CountDownLatch* connectionCloseLatch);
  virtual ~DummyMtrMgr();

  heron::proto::tmanager::TManagerLocation* get_tmanager();

 protected:
  // handle an incoming connection from server
  virtual void HandleNewConnection(Connection* newConnection);

  // handle a connection close
  virtual void HandleConnectionClose(Connection* connection, NetworkErrorCode status);

  // Handle metrics publisher request
  virtual void HandleMetricPublisherRegisterRequest(REQID _id, Connection* _conn,
                  pool_unique_ptr<heron::proto::system::MetricPublisherRegisterRequest> _request);
  virtual void HandleMetricPublisherPublishMessage(
      Connection* _conn,
      pool_unique_ptr<heron::proto::system::MetricPublisherPublishMessage> _message);
  virtual void HandleTManagerLocationMessage(
      Connection*, pool_unique_ptr<heron::proto::system::TManagerLocationRefreshMessage> _message);

 private:
  sp_string stmgr_id_expected_;
  heron::proto::tmanager::TManagerLocation* location_;
  // Used to signal that tmanager location has been received
  CountDownLatch* tmanagerLatch_;
  // Used to signal that connection to stmgr has been closed
  CountDownLatch* connectionCloseLatch_;
};

#endif
