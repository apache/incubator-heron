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

#include <iostream>
#include <vector>

#include "proto/messages.h"
#include "glog/logging.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "server/dummy_metricsmgr.h"
///////////////////////////// DummyMtrMgr /////////////////////////////////////////////////
DummyMtrMgr::DummyMtrMgr(EventLoopImpl* ss, const NetworkOptions& options,
                         const sp_string& stmgr_id, CountDownLatch* tmasterLatch,
                         CountDownLatch* connectionCloseLatch)
    : Server(ss, options),
      stmgr_id_expected_(stmgr_id),
      location_(NULL),
      tmasterLatch_(tmasterLatch),
      connectionCloseLatch_(connectionCloseLatch) {
  InstallRequestHandler(&DummyMtrMgr::HandleMetricPublisherRegisterRequest);
  InstallMessageHandler(&DummyMtrMgr::HandleMetricPublisherPublishMessage);
  InstallMessageHandler(&DummyMtrMgr::HandleTMasterLocationMessage);
}

DummyMtrMgr::~DummyMtrMgr() { delete location_; }

void DummyMtrMgr::HandleNewConnection(Connection* conn) { LOG(INFO) << "Got a new connection"; }

void DummyMtrMgr::HandleConnectionClose(Connection*, NetworkErrorCode status) {
  LOG(INFO) << "Got a connection close, status = " << status;

  if (connectionCloseLatch_ != NULL) {
    // Notify that we have successfully closed the connection
    connectionCloseLatch_->countDown();
  }
}

void DummyMtrMgr::HandleMetricPublisherRegisterRequest(
    REQID id, Connection* conn, heron::proto::system::MetricPublisherRegisterRequest* request) {
  LOG(INFO) << "Got a register request ";
  heron::proto::system::MetricPublisherRegisterResponse response;
  response.mutable_status()->set_status(heron::proto::system::OK);
  SendResponse(id, conn, response);
  delete request;
}

void DummyMtrMgr::HandleMetricPublisherPublishMessage(
    Connection*, heron::proto::system::MetricPublisherPublishMessage* message) {
  delete message;
}

void DummyMtrMgr::HandleTMasterLocationMessage(
    Connection*, heron::proto::system::TMasterLocationRefreshMessage* message) {
  location_ = message->release_tmaster();
  delete message;

  LOG(INFO) << "Got tmaster location: " << location_->host() << ":" << location_->master_port();

  if (tmasterLatch_ != NULL) {
    // notify that we received tmaster location
    tmasterLatch_->countDown();
  }
}

heron::proto::tmaster::TMasterLocation* DummyMtrMgr::get_tmaster() { return location_; }
