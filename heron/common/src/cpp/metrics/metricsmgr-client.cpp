/*
 * Copyright 2015 Twitter, Inc.
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

#include "metrics/metricsmgr-client.h"
#include <stdio.h>
#include <iostream>
#include <string>
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace common {

MetricsMgrClient::MetricsMgrClient(const sp_string& _hostname, sp_int32 _port,
                                   const sp_string& _component_id, const sp_string& _task_id,
                                   EventLoop* eventLoop, const NetworkOptions& _options)
    : Client(eventLoop, _options),
      hostname_(_hostname),
      port_(_port),
      component_id_(_component_id),
      task_id_(_task_id),
      tmaster_location_(NULL),
      registered_(false) {
  InstallResponseHandler(new proto::system::MetricPublisherRegisterRequest(),
                         &MetricsMgrClient::HandleRegisterResponse);
  Start();
}

MetricsMgrClient::~MetricsMgrClient() { delete tmaster_location_; }

void MetricsMgrClient::HandleConnect(NetworkErrorCode _status) {
  if (_status == OK) {
    LOG(INFO) << "Connected to metrics manager" << std::endl;
    SendRegisterRequest();
  } else {
    LOG(ERROR) << "Could not connect to metrics mgr. Will Retry in 1 second\n";
    AddTimer([this]() { this->ReConnect(); }, 1000000);
  }
}

void MetricsMgrClient::HandleClose(NetworkErrorCode) {
  LOG(ERROR) << "Metrics Mgr closed connection on us. Will Retry in 1 second\n";
  AddTimer([this]() { this->ReConnect(); }, 1000000);
  registered_ = false;
}

void MetricsMgrClient::ReConnect() { Start(); }

void MetricsMgrClient::SendRegisterRequest() {
  proto::system::MetricPublisherRegisterRequest* request;
  request = new proto::system::MetricPublisherRegisterRequest();

  proto::system::MetricPublisher* publisher = request->mutable_publisher();
  publisher->set_hostname(hostname_);
  publisher->set_port(port_);
  publisher->set_component_name(component_id_);
  publisher->set_instance_id(task_id_);
  publisher->set_instance_index(-1);

  SendRequest(request, NULL);
}

void MetricsMgrClient::HandleRegisterResponse(
    void*, proto::system::MetricPublisherRegisterResponse* _response, NetworkErrorCode _status) {
  if (_status == OK && _response->status().status() != proto::system::OK) {
    // What the heck we explicitly got a non ok response
    LOG(ERROR) << "Recieved a non-ok status from metrics mgr" << std::endl;
    ::exit(1);
  } else {
    LOG(INFO) << "Successfully registered ourselves to the metricsmgr";
    registered_ = true;
  }
  delete _response;
  // Check if we need to send tmaster location
  if (tmaster_location_) {
    LOG(INFO) << "Sending TMaster Location to metricsmgr";
    InternalSendTMasterLocation();
  } else {
    LOG(INFO) << "Do not have a TMasterLocation yet";
  }
}

void MetricsMgrClient::SendTMasterLocation(const proto::tmaster::TMasterLocation& location) {
  if (tmaster_location_) {
    delete tmaster_location_;
  }
  tmaster_location_ = new proto::tmaster::TMasterLocation(location);
  if (registered_) {
    LOG(INFO) << "Sending TMaster Location to metricsmgr";
    InternalSendTMasterLocation();
  } else {
    LOG(INFO) << "We have not yet registered to metricsmgr."
              << " Holding off sending TMasterLocation";
  }
}

void MetricsMgrClient::SendMetrics(proto::system::MetricPublisherPublishMessage* _message) {
  SendMessage(*_message);

  delete _message;
}

void MetricsMgrClient::InternalSendTMasterLocation() {
  CHECK(tmaster_location_);
  proto::system::TMasterLocationRefreshMessage* m =
      new proto::system::TMasterLocationRefreshMessage();
  m->mutable_tmaster()->CopyFrom(*tmaster_location_);
  SendMessage(*m);

  delete m;
}
}  // namespace common
}  // namespace heron
