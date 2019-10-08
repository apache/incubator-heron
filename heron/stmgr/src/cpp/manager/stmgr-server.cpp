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

#include "manager/stmgr-server.h"
#include <iostream>
#include <unordered_set>
#include <string>
#include <vector>
#include "manager/stmgr.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/helper.h"
#include "config/heron-internals-config-reader.h"
#include "metrics/metrics.h"

namespace heron {
namespace stmgr {

using std::make_shared;

// The scope the metrics in this file are under
const sp_string SERVER_SCOPE = "__server/";
// Num data tuples received from other stream managers
const sp_string METRIC_DATA_TUPLES_FROM_STMGRS = "__tuples_from_stmgrs";
// Num ack tuples received from other stream managers
const sp_string METRIC_ACK_TUPLES_FROM_STMGRS = "__ack_tuples_from_stmgrs";
// Num fail tuples received from other stream managers
const sp_string METRIC_FAIL_TUPLES_FROM_STMGRS = "__fail_tuples_from_stmgrs";
// Bytes received from other stream managers
const sp_string METRIC_BYTES_FROM_STMGRS = "__bytes_from_stmgrs";
// Time spent in back pressure caused by remote stream managers.
const sp_string METRIC_TIME_SPENT_BACK_PRESSURE_CAUSED_BY_REMOTE_STMGR =
    "__time_spent_back_pressure_by_remote_stmgr";


StMgrServer::StMgrServer(shared_ptr<EventLoop> eventLoop, const NetworkOptions& _options,
                         const sp_string& _topology_name, const sp_string& _topology_id,
                         const sp_string& _stmgr_id, StMgr* _stmgr,
                         shared_ptr<heron::common::MetricsMgrSt> const& _metrics_manager_client)
    : Server(eventLoop, _options),
      topology_name_(_topology_name),
      topology_id_(_topology_id),
      stmgr_id_(_stmgr_id),
      stmgr_(_stmgr),
      metrics_manager_client_(_metrics_manager_client) {
  // stmgr related handlers
  InstallRequestHandler(&StMgrServer::HandleStMgrHelloRequest);
  InstallMessageHandler(&StMgrServer::HandleTupleStreamMessage);
  InstallMessageHandler(&StMgrServer::HandleStartBackPressureMessage);
  InstallMessageHandler(&StMgrServer::HandleStopBackPressureMessage);
  InstallMessageHandler(&StMgrServer::HandleDownstreamStatefulCheckpointMessage);

  // The metrics need to be registered one by one here because the "__server" scope
  // is already registered in heron::stmgr::InstanceServer. Duplicated registrations
  // will only have one successfully registered.
  tuples_from_stmgrs_metrics_ = make_shared<heron::common::CountMetric>();
  metrics_manager_client_->register_metric(SERVER_SCOPE + METRIC_DATA_TUPLES_FROM_STMGRS,
                                           tuples_from_stmgrs_metrics_);
  ack_tuples_from_stmgrs_metrics_ = make_shared<heron::common::CountMetric>();
  metrics_manager_client_->register_metric(SERVER_SCOPE + METRIC_ACK_TUPLES_FROM_STMGRS,
                                           ack_tuples_from_stmgrs_metrics_);
  fail_tuples_from_stmgrs_metrics_ = make_shared<heron::common::CountMetric>();
  metrics_manager_client_->register_metric(SERVER_SCOPE + METRIC_FAIL_TUPLES_FROM_STMGRS,
                                           fail_tuples_from_stmgrs_metrics_);
  bytes_from_stmgrs_metrics_ = make_shared<heron::common::CountMetric>();
  metrics_manager_client_->register_metric(SERVER_SCOPE + METRIC_BYTES_FROM_STMGRS,
                                           bytes_from_stmgrs_metrics_);
  back_pressure_metric_caused_by_remote_stmgr_ = make_shared<heron::common::TimeSpentMetric>();
  metrics_manager_client_->register_metric(
      SERVER_SCOPE + METRIC_TIME_SPENT_BACK_PRESSURE_CAUSED_BY_REMOTE_STMGR,
      back_pressure_metric_caused_by_remote_stmgr_);
}

StMgrServer::~StMgrServer() {
  Stop();
  metrics_manager_client_->unregister_metric(SERVER_SCOPE + METRIC_DATA_TUPLES_FROM_STMGRS);
  metrics_manager_client_->unregister_metric(SERVER_SCOPE + METRIC_ACK_TUPLES_FROM_STMGRS);
  metrics_manager_client_->unregister_metric(SERVER_SCOPE + METRIC_FAIL_TUPLES_FROM_STMGRS);
  metrics_manager_client_->unregister_metric(SERVER_SCOPE + METRIC_BYTES_FROM_STMGRS);
  metrics_manager_client_->unregister_metric(
      SERVER_SCOPE + METRIC_TIME_SPENT_BACK_PRESSURE_CAUSED_BY_REMOTE_STMGR);
}

void StMgrServer::HandleNewConnection(Connection* _conn) {
  // There is nothing to be done here. Instead we wait
  // for the register/hello
  LOG(INFO) << "StMgrServer Got new connection " << _conn << " from "
            << _conn->getIPAddress() << ":" << _conn->getPort();
}

void StMgrServer::HandleConnectionClose(Connection* _conn, NetworkErrorCode) {
  LOG(INFO) << "StMgrServer Got connection close of " << _conn << " from "
            << _conn->getIPAddress() << ":" << _conn->getPort();
  // Find the stmgr who hung up
  auto siter = rstmgrs_.find(_conn);
  if (siter == rstmgrs_.end()) {
    LOG(ERROR) << "StMgrServer could not identity connection " << _conn << " from "
               << _conn->getIPAddress() << ":" << _conn->getPort();
    return;
  }
  // This is a stmgr connection
  LOG(INFO) << "Stmgr " << siter->second << " closed connection";
  sp_string stmgr_id = rstmgrs_[_conn];
  // Did we receive a start back pressure message from this stmgr to
  // begin with?
  if (stmgrs_who_announced_back_pressure_.find(stmgr_id) !=
      stmgrs_who_announced_back_pressure_.end()) {
    stmgrs_who_announced_back_pressure_.erase(stmgr_id);
    if (stmgrs_who_announced_back_pressure_.empty()) {
      stmgr_->AttemptStopBackPressureFromSpouts();
    }
  }
  // Now cleanup the data structures
  stmgrs_.erase(siter->second);
  rstmgrs_.erase(_conn);
}

void StMgrServer::HandleStMgrHelloRequest(REQID _id, Connection* _conn,
                                       pool_unique_ptr<proto::stmgr::StrMgrHelloRequest> _request) {
  LOG(INFO) << "Got a hello message from stmgr " << _request->stmgr() << " on connection " << _conn;
  proto::stmgr::StrMgrHelloResponse response;
  // Some basic checks
  if (_request->topology_name() != topology_name_) {
    LOG(ERROR) << "The hello message was from a different topology " << _request->topology_name()
               << std::endl;
    response.mutable_status()->set_status(proto::system::NOTOK);
  } else if (_request->topology_id() != topology_id_) {
    LOG(ERROR) << "The hello message was from a different topology id " << _request->topology_id()
               << std::endl;
    response.mutable_status()->set_status(proto::system::NOTOK);
  } else if (stmgrs_.find(_request->stmgr()) != stmgrs_.end()) {
    LOG(WARNING) << "We already had an active connection from the stmgr " << _request->stmgr()
                 << ". Closing existing connection...";
    // This will free up the slot in the various maps in this class
    // and the next time around we'll be able to add this stmgr.
    // We shouldn't add the new stmgr connection right now because
    // the close could be asynchronous (fired through a 0 timer)
    stmgrs_[_request->stmgr()]->closeConnection();
    response.mutable_status()->set_status(proto::system::NOTOK);
  } else {
    stmgrs_[_request->stmgr()] = _conn;
    rstmgrs_[_conn] = _request->stmgr();
    response.mutable_status()->set_status(proto::system::OK);
  }
  SendResponse(_id, _conn, response);
}

void StMgrServer::HandleTupleStreamMessage(Connection* _conn,
                                       pool_unique_ptr<proto::stmgr::TupleStreamMessage> _message) {
  auto iter = rstmgrs_.find(_conn);
  if (iter == rstmgrs_.end()) {
    LOG(INFO) << "Recieved Tuple messages from unknown streammanager connection";
  } else {
    proto::system::HeronTupleSet2* tuple_set = nullptr;
    tuple_set = __global_protobuf_pool_acquire__(tuple_set);
    tuple_set->ParsePartialFromString(_message->set());

    bytes_from_stmgrs_metrics_->incr_by(_message->ByteSize());
    if (tuple_set->has_data()) {
      tuples_from_stmgrs_metrics_->incr_by(tuple_set->data().tuples_size());
    } else if (tuple_set->has_control()) {
      ack_tuples_from_stmgrs_metrics_->incr_by(tuple_set->control().acks_size());
      fail_tuples_from_stmgrs_metrics_->incr_by(tuple_set->control().fails_size());
    }
    __global_protobuf_pool_release__(tuple_set);

    stmgr_->HandleStreamManagerData(iter->second, std::move(_message));
  }
}

void StMgrServer::StartBackPressureClientCb(const sp_string& _other_stmgr_id) {
  if (!stmgr_->DidAnnounceBackPressure()) {
    stmgr_->SendStartBackPressureToOtherStMgrs();
    // Start backpressure from remote stmgr metric
    back_pressure_metric_caused_by_remote_stmgr_->Start();
  }
  remote_ends_who_caused_back_pressure_.insert(_other_stmgr_id);
  LOG(WARNING) << "We observe back pressure on sending data to remote stream manager "
               << _other_stmgr_id;
  stmgr_->StartBackPressureOnSpouts();
}

void StMgrServer::StopBackPressureClientCb(const sp_string& _other_stmgr_id) {
  CHECK(remote_ends_who_caused_back_pressure_.find(_other_stmgr_id) !=
        remote_ends_who_caused_back_pressure_.end());
  remote_ends_who_caused_back_pressure_.erase(_other_stmgr_id);

  if (!stmgr_->DidAnnounceBackPressure()) {
    stmgr_->SendStopBackPressureToOtherStMgrs();
    // Stop backpressure from remote stmgr metric
    back_pressure_metric_caused_by_remote_stmgr_->Stop();
  }
  LOG(WARNING) << "We don't observe back pressure now on sending data to remote "
                  "stream manager "
            << _other_stmgr_id;
  if (!stmgr_->DidAnnounceBackPressure() && !stmgr_->DidOthersAnnounceBackPressure()) {
    stmgr_->AttemptStopBackPressureFromSpouts();
  }
}

void StMgrServer::HandleStartBackPressureMessage(Connection* _conn,
                               pool_unique_ptr<proto::stmgr::StartBackPressureMessage> _message) {
  // Close spouts
  LOG(INFO) << "Received start back pressure from str mgr " << _message->stmgr();
  if (_message->topology_name() != topology_name_ || _message->topology_id() != topology_id_) {
    LOG(ERROR) << "Received start back pressure message from unknown stream manager "
               << _message->topology_name() << " " << _message->topology_id() << " "
               << _message->stmgr() << " " << _message->message_id();
    return;
  }
  auto iter = rstmgrs_.find(_conn);
  CHECK(iter != rstmgrs_.end());
  sp_string stmgr_id = iter->second;
  stmgrs_who_announced_back_pressure_.insert(stmgr_id);

  stmgr_->StartBackPressureOnSpouts();
}

void StMgrServer::HandleStopBackPressureMessage(Connection* _conn,
                                pool_unique_ptr<proto::stmgr::StopBackPressureMessage> _message) {
  LOG(INFO) << "Received stop back pressure from str mgr " << _message->stmgr();
  if (_message->topology_name() != topology_name_ || _message->topology_id() != topology_id_) {
    LOG(ERROR) << "Received stop back pressure message from unknown stream manager "
               << _message->topology_name() << " " << _message->topology_id() << " "
               << _message->stmgr();
    return;
  }

  auto iter = rstmgrs_.find(_conn);
  CHECK(iter != rstmgrs_.end());
  sp_string stmgr_id = iter->second;
  // Did we receive a start back pressure message from this stmgr to
  // begin with? We could have been dead at the time of the announcement
  if (stmgrs_who_announced_back_pressure_.find(stmgr_id) !=
      stmgrs_who_announced_back_pressure_.end()) {
    stmgrs_who_announced_back_pressure_.erase(stmgr_id);
  }
  if (!stmgr_->DidAnnounceBackPressure() && !stmgr_->DidOthersAnnounceBackPressure()) {
    stmgr_->AttemptStopBackPressureFromSpouts();
  }
}

void StMgrServer::HandleDownstreamStatefulCheckpointMessage(Connection* _conn,
                          pool_unique_ptr<proto::ckptmgr::DownstreamStatefulCheckpoint> _message) {
  stmgr_->HandleDownStreamStatefulCheckpoint(*_message);
}

}  // namespace stmgr
}  // namespace heron
