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

#include "manager/stmgr-client.h"
#include <stdio.h>
#include <iostream>
#include <string>
#include "manager/stmgr-clientmgr.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/heron-internals-config-reader.h"
#include "metrics/metrics.h"


namespace heron {
namespace stmgr {

// Num data tuples sent to other stream managers
const sp_string METRIC_DATA_TUPLES_TO_STMGRS = "__tuples_to_stmgrs";
// Num ack tuples sent to other stream managers
const sp_string METRIC_ACK_TUPLES_TO_STMGRS = "__ack_tuples_to_stmgrs";
// Num fail tuples sent to other stream managers
const sp_string METRIC_FAIL_TUPLES_TO_STMGRS = "__fail_tuples_to_stmgrs";
// Num data tuples lost while sending to other stmgrs
const sp_string METRIC_DATA_TUPLES_TO_STMGRS_LOST = "__tuples_to_stmgrs_lost";
// Num ack tuples lost while sending to other stream managers
const sp_string METRIC_ACK_TUPLES_TO_STMGRS_LOST = "__ack_tuples_to_stmgrs_lost";
// Num fail tuples lost while sending to other stream managers
const sp_string METRIC_FAIL_TUPLES_TO_STMGRS_LOST = "__fail_tuples_to_stmgrs_lost";
// Bytes sent to other stream managers
const sp_string METRIC_BYTES_TO_STMGRS = "__bytes_to_stmgrs";
// Bytes lost to other stream managers
const sp_string METRIC_BYTES_TO_STMGRS_LOST = "__bytes_to_stmgrs_lost";
// Number of times we send hello messages to stmgrs
const sp_string METRIC_HELLO_MESSAGES_TO_STMGRS = "__hello_messages_to_stmgrs";

StMgrClient::StMgrClient(EventLoop* eventLoop, const NetworkOptions& _options,
                         const sp_string& _topology_name, const sp_string& _topology_id,
                         const sp_string& _our_id, const sp_string& _other_id,
                         StMgrClientMgr* _client_manager,
                         heron::common::MetricsMgrSt* _metrics_manager_client)
    : Client(eventLoop, _options),
      topology_name_(_topology_name),
      topology_id_(_topology_id),
      our_stmgr_id_(_our_id),
      other_stmgr_id_(_other_id),
      quit_(false),
      client_manager_(_client_manager),
      metrics_manager_client_(_metrics_manager_client),
      ndropped_messages_(0),
      reconnect_attempts_(0),
      is_registered_(false) {
  reconnect_other_streammgrs_interval_sec_ =
      config::HeronInternalsConfigReader::Instance()->GetHeronStreammgrClientReconnectIntervalSec();
  reconnect_other_streammgrs_max_attempt_ =
      config::HeronInternalsConfigReader::Instance()->GetHeronStreammgrClientReconnectMaxAttempts();

  InstallResponseHandler(new proto::stmgr::StrMgrHelloRequest(), &StMgrClient::HandleHelloResponse);
  InstallMessageHandler(&StMgrClient::HandleTupleStreamMessage);

  stmgr_client_metrics_ = new heron::common::MultiCountMetric();
  metrics_manager_client_->register_metric("__client_" + other_stmgr_id_, stmgr_client_metrics_);
}

StMgrClient::~StMgrClient() {
  Stop();
  metrics_manager_client_->unregister_metric("__client_" + other_stmgr_id_);
  delete stmgr_client_metrics_;
}

void StMgrClient::Quit() {
  quit_ = true;
  Stop();
}

void StMgrClient::HandleConnect(NetworkErrorCode _status) {
  if (_status == OK) {
    LOG(INFO) << "Connected to stmgr " << other_stmgr_id_ << " running at "
              << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
              << std::endl;

    // reset the reconnect attempt once connection established
    reconnect_attempts_ = 0;

    if (quit_) {
      Stop();
    } else {
      SendHelloRequest();
    }
  } else {
    LOG(WARNING) << "Could not connect to stmgr " << other_stmgr_id_ << " running at "
                 << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
                 << " due to: " << _status << std::endl;
    if (quit_) {
      LOG(ERROR) << "Instructed to quit. Quitting...";
      delete this;
      return;
    } else {
      LOG(INFO) << "Retrying again..." << std::endl;
      AddTimer([this]() { this->OnReConnectTimer(); },
               reconnect_other_streammgrs_interval_sec_ * 1000 * 1000);
    }
  }
}

void StMgrClient::HandleClose(NetworkErrorCode _code) {
  is_registered_ = false;
  if (_code == OK) {
    LOG(INFO) << "We closed our server connection with stmgr " << other_stmgr_id_ << " running at "
              << get_clientoptions().get_host() << ":" << get_clientoptions().get_port()
              << std::endl;
  } else {
    LOG(INFO) << "Stmgr " << other_stmgr_id_ << " running at " << get_clientoptions().get_host()
              << ":" << get_clientoptions().get_port() << " closed connection with code " << _code
              << std::endl;
  }
  if (quit_) {
    delete this;
  } else {
    client_manager_->HandleDeadStMgrConnection(other_stmgr_id_);
    LOG(INFO) << "Will try to reconnect again after 1 seconds" << std::endl;
    AddTimer([this]() { this->OnReConnectTimer(); },
             reconnect_other_streammgrs_interval_sec_ * 1000 * 1000);
  }
}

void StMgrClient::HandleHelloResponse(void*, proto::stmgr::StrMgrHelloResponse* _response,
                                      NetworkErrorCode _status) {
  if (_status != OK) {
    LOG(ERROR) << "NonOK network code " << _status << " for register response from stmgr "
               << other_stmgr_id_ << " running at " << get_clientoptions().get_host() << ":"
               << get_clientoptions().get_port();
    __global_protobuf_pool_release__(_response);
    Stop();
    return;
  }
  proto::system::StatusCode status = _response->status().status();
  if (status != proto::system::OK) {
    LOG(ERROR) << "NonOK register response " << status << " from stmgr " << other_stmgr_id_
               << " running at " << get_clientoptions().get_host() << ":"
               << get_clientoptions().get_port();
    __global_protobuf_pool_release__(_response);
    Stop();
    return;
  }
  __global_protobuf_pool_release__(_response);
  is_registered_ = true;
  if (client_manager_->DidAnnounceBackPressure()) {
    SendStartBackPressureMessage();
  }
  client_manager_->HandleStMgrClientRegistered();
}

void StMgrClient::OnReConnectTimer() {
  reconnect_attempts_ += 1;

  if (reconnect_attempts_ < reconnect_other_streammgrs_max_attempt_) {
    Start();
  } else {
    LOG(FATAL) << "Could not connect to stmgr " << other_stmgr_id_
               << " after reaching the max reconnect attempts. Quitting...";
  }
}

void StMgrClient::SendHelloRequest() {
  auto request = new proto::stmgr::StrMgrHelloRequest();
  request->set_topology_name(topology_name_);
  request->set_topology_id(topology_id_);
  request->set_stmgr(our_stmgr_id_);
  SendRequest(request, NULL);
  stmgr_client_metrics_->scope(METRIC_HELLO_MESSAGES_TO_STMGRS)->incr_by(1);
  return;
}

bool StMgrClient::SendTupleStreamMessage(proto::stmgr::TupleStreamMessage& _msg) {
  if (IsConnected()) {
    SendMessage(_msg);
    return true;
  } else {
    if (++ndropped_messages_ % 100 == 0) {
      LOG(INFO) << "Dropping " << ndropped_messages_ << "th tuple message to stmgr "
                << other_stmgr_id_ << " because it is not connected";
    }
    return false;
  }
}

void StMgrClient::HandleTupleStreamMessage(proto::stmgr::TupleStreamMessage* _message) {
  __global_protobuf_pool_release__(_message);
  LOG(FATAL) << "We should not receive tuple messages in the client" << std::endl;
}

void StMgrClient::StartBackPressureConnectionCb(Connection* _connection) {
  _connection->setCausedBackPressure();
  // Ask the StMgrServer to stop consuming. The client does
  // not consume anything

  client_manager_->StartBackPressureOnServer(other_stmgr_id_);
}

void StMgrClient::StopBackPressureConnectionCb(Connection* _connection) {
  _connection->unsetCausedBackPressure();
  // Call the StMgrServers removeBackPressure method
  client_manager_->StopBackPressureOnServer(other_stmgr_id_);
}

void StMgrClient::SendStartBackPressureMessage() {
  REQID_Generator generator;
  REQID rand = generator.generate();
  // generator.generate(rand);
  proto::stmgr::StartBackPressureMessage* message = nullptr;
  message = __global_protobuf_pool_acquire__(message);
  message->set_topology_name(topology_name_);
  message->set_topology_id(topology_id_);
  message->set_stmgr(our_stmgr_id_);
  message->set_message_id(rand.str());
  SendMessage(*message);

  __global_protobuf_pool_release__(message);
}

void StMgrClient::SendStopBackPressureMessage() {
  REQID_Generator generator;
  REQID rand = generator.generate();
  // generator.generate(rand);
  proto::stmgr::StopBackPressureMessage* message = nullptr;
  message = __global_protobuf_pool_acquire__(message);
  message->set_topology_name(topology_name_);
  message->set_topology_id(topology_id_);
  message->set_stmgr(our_stmgr_id_);
  message->set_message_id(rand.str());
  SendMessage(*message);

  __global_protobuf_pool_release__(message);
}

void StMgrClient::SendDownstreamStatefulCheckpoint(
                  proto::ckptmgr::DownstreamStatefulCheckpoint* _message) {
  LOG(INFO) << "Sending Downstream Checkpoint message for src_task_id: "
            << _message->origin_task_id() << " dest_task_id: "
            << _message->destination_task_id() << " checkpoint: "
            << _message->checkpoint_id();
  SendMessage(*_message);
  __global_protobuf_pool_release__(_message);
}
}  // namespace stmgr
}  // namespace heron
