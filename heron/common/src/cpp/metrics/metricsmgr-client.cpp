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

using std::shared_ptr;

MetricsMgrClient::MetricsMgrClient(const sp_string& _hostname, sp_int32 _port,
                                   const sp_string& _component_name, const sp_string& _instance_id,
                                   int _instance_index,
                                   shared_ptr<EventLoop> eventLoop, const NetworkOptions& _options)
    : Client(eventLoop, _options),
      hostname_(_hostname),
      port_(_port),
      component_name_(_component_name),
      instance_id_(_instance_id),
      instance_index_(_instance_index),
      tmanager_location_(NULL),
      metricscache_location_(NULL),
      registered_(false) {
  InstallResponseHandler(make_unique<proto::system::MetricPublisherRegisterRequest>(),
                         &MetricsMgrClient::HandleRegisterResponse);
  Start();
}

MetricsMgrClient::~MetricsMgrClient() { delete tmanager_location_; delete metricscache_location_; }

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
  auto request = make_unique<proto::system::MetricPublisherRegisterRequest>();

  proto::system::MetricPublisher* publisher = request->mutable_publisher();
  publisher->set_hostname(hostname_);
  publisher->set_port(port_);
  publisher->set_component_name(component_name_);
  publisher->set_instance_id(instance_id_);
  publisher->set_instance_index(instance_index_);

  SendRequest(std::move(request), nullptr);
}

void MetricsMgrClient::HandleRegisterResponse(
    void*, pool_unique_ptr<proto::system::MetricPublisherRegisterResponse> _response,
    NetworkErrorCode _status) {
  if (_status == OK && _response->status().status() != proto::system::OK) {
    // What the heck we explicitly got a non ok response
    LOG(ERROR) << "Recieved a non-ok status from metrics mgr" << std::endl;
    ::exit(1);
  } else {
    LOG(INFO) << "Successfully registered ourselves to the metricsmgr";
    registered_ = true;
  }

  // Check if we need to send tmanager location
  if (tmanager_location_) {
    LOG(INFO) << "Sending TManager Location to metricsmgr";
    InternalSendTManagerLocation();
  } else {
    LOG(INFO) << "Do not have a TManagerLocation yet";
  }
  // Check if we need to send metricscache location
  if (metricscache_location_) {
    LOG(INFO) << "Sending MetricsCache Location to metricsmgr";
    InternalSendMetricsCacheLocation();
  } else {
    LOG(INFO) << "Do not have a MetricsCacheLocation yet";
  }
}

void MetricsMgrClient::SendTManagerLocation(const proto::tmanager::TManagerLocation& location) {
  if (tmanager_location_) {
    delete tmanager_location_;
  }
  tmanager_location_ = new proto::tmanager::TManagerLocation(location);
  if (registered_) {
    LOG(INFO) << "Sending TManager Location to metricsmgr";
    InternalSendTManagerLocation();
  } else {
    LOG(INFO) << "We have not yet registered to metricsmgr."
              << " Holding off sending TManagerLocation";
  }
}

void MetricsMgrClient::SendMetricsCacheLocation(
    const proto::tmanager::MetricsCacheLocation& location) {
  if (metricscache_location_) {
    delete metricscache_location_;
  }
  metricscache_location_ = new proto::tmanager::MetricsCacheLocation(location);
  if (registered_) {
    LOG(INFO) << "Sending MetricsCache Location to metricsmgr";
    InternalSendMetricsCacheLocation();
  } else {
    LOG(INFO) << "We have not yet registered to metricsmgr."
              << " Holding off sending MetricsCacheLocation";
  }
}

void MetricsMgrClient::SendMetrics(proto::system::MetricPublisherPublishMessage* _message) {
  SendMessage(*_message);

  delete _message;
}

void MetricsMgrClient::InternalSendTManagerLocation() {
  CHECK(tmanager_location_);
  proto::system::TManagerLocationRefreshMessage* m =
      new proto::system::TManagerLocationRefreshMessage();
  m->mutable_tmanager()->CopyFrom(*tmanager_location_);
  SendMessage(*m);

  delete m;
}

void MetricsMgrClient::InternalSendMetricsCacheLocation() {
  CHECK(metricscache_location_);
  proto::system::MetricsCacheLocationRefreshMessage* m =
      new proto::system::MetricsCacheLocationRefreshMessage();
  m->mutable_metricscache()->CopyFrom(*metricscache_location_);
  SendMessage(*m);

  delete m;
}

}  // namespace common
}  // namespace heron
