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

#include "manager/stmgr-server.h"
#include <iostream>
#include <set>
#include <vector>
#include "manager/stmgr.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/helper.h"
#include "metrics/metrics.h"

namespace heron {
namespace stmgr {

// Num data tuples received from other stream managers
const sp_string METRIC_DATA_TUPLES_FROM_STMGRS = "__tuples_from_stmgrs";
// Num ack tuples received from other stream managers
const sp_string METRIC_ACK_TUPLES_FROM_STMGRS = "__ack_tuples_from_stmgrs";
// Num fail tuples received from other stream managers
const sp_string METRIC_FAIL_TUPLES_FROM_STMGRS = "__fail_tuples_from_stmgrs";
// Bytes received from other stream managers
const sp_string METRIC_BYTES_FROM_STMGRS = "__bytes_from_stmgrs";
// Num data tuples sent to instances associated with this stream manager
const sp_string METRIC_DATA_TUPLES_TO_INSTANCES = "__tuples_to_workers";
// Num ack tuples sent to instances associated with this stream manager
const sp_string METRIC_ACK_TUPLES_TO_INSTANCES = "__ack_tuples_to_workers";
// Num fail tuples sent to instances associated with this stream manager
const sp_string METRIC_FAIL_TUPLES_TO_INSTANCES = "__fail_tuples_to_workers";
// Bytes sent to instances
const sp_string METRIC_BYTES_TO_INSTANCES = "__bytes_to_workers";
// Num data tuples from instances associated with this stream manager
const sp_string METRIC_DATA_TUPLES_FROM_INSTANCES = "__tuples_from_workers";
// Num ack tuples from instances associated with this stream manager
const sp_string METRIC_ACK_TUPLES_FROM_INSTANCES = "__ack_tuples_from_workers";
// Num fail tuples from instances associated with this stream manager
const sp_string METRIC_FAIL_TUPLES_FROM_INSTANCES = "__fail_tuples_from_workers";
// Bytes received from instances
const sp_string METRIC_BYTES_FROM_INSTANCES = "__bytes_from_workers";
// Num data tuples lost since instances is not connected
const sp_string METRIC_DATA_TUPLES_TO_INSTANCES_LOST = "__tuples_to_workers_lost";
// Num ack tuples lost since instances is not connected
const sp_string METRIC_ACK_TUPLES_TO_INSTANCES_LOST = "__ack_tuples_to_workers_lost";
// Num fail tuples lost since instances is not connected
const sp_string METRIC_FAIL_TUPLES_TO_INSTANCES_LOST = "__fail_tuples_to_workers_lost";
// Num bytes lost since instances is not connected
const sp_string METRIC_BYTES_TO_INSTANCES_LOST = "__bytes_to_workers_lost";
// Time spent in back pressure aggregated - back pressure initiated by us +
// others
const sp_string METRIC_TIME_SPENT_BACK_PRESSURE_AGGR = "__server/__time_spent_back_pressure_aggr";
// Time spent in back pressure because of local instances connection;
// we initiated this backpressure
const sp_string METRIC_TIME_SPENT_BACK_PRESSURE_INIT =
    "__server/__time_spent_back_pressure_initiated";
// Time spent in back pressure because of a component id. The comp id will be
// appended
// to the string below
const sp_string METRIC_TIME_SPENT_BACK_PRESSURE_COMPID = "__time_spent_back_pressure_by_compid/";
// Queue size in bytes sent to each instance
const sp_string METRIC_QUEUE_SIZE_TO_INSTANCE_COMPID = "__queue_size_bytes_to_instance_by_compid/";

StMgrServer::StMgrServer(EventLoop* eventLoop, const NetworkOptions& _options,
                         const sp_string& _topology_name, const sp_string& _topology_id,
                         const sp_string& _stmgr_id,
                         const std::vector<sp_string>& _expected_instances, StMgr* _stmgr,
                         heron::common::MetricsMgrSt* _metrics_manager_client)
    : Server(eventLoop, _options),
      topology_name_(_topology_name),
      topology_id_(_topology_id),
      stmgr_id_(_stmgr_id),
      expected_instances_(_expected_instances),
      stmgr_(_stmgr),
      metrics_manager_client_(_metrics_manager_client) {
  // stmgr related handlers
  InstallRequestHandler(&StMgrServer::HandleStMgrHelloRequest);
  InstallMessageHandler(&StMgrServer::HandleTupleStreamMessage);
  InstallMessageHandler(&StMgrServer::HandleStartBackPressureMessage);
  InstallMessageHandler(&StMgrServer::HandleStopBackPressureMessage);

  // instance related handlers
  InstallRequestHandler(&StMgrServer::HandleRegisterInstanceRequest);
  InstallMessageHandler(&StMgrServer::HandleTupleSetMessage);

  stmgr_server_metrics_ = new heron::common::MultiCountMetric();
  back_pressure_metric_aggr_ = new heron::common::TimeSpentMetric();
  back_pressure_metric_initiated_ = new heron::common::TimeSpentMetric();
  metrics_manager_client_->register_metric("__server", stmgr_server_metrics_);
  metrics_manager_client_->register_metric(METRIC_TIME_SPENT_BACK_PRESSURE_AGGR,
                                           back_pressure_metric_aggr_);
  metrics_manager_client_->register_metric(METRIC_TIME_SPENT_BACK_PRESSURE_INIT,
                                           back_pressure_metric_initiated_);
  spouts_under_back_pressure_ = false;
}

StMgrServer::~StMgrServer() {
  Stop();
  // Unregister and delete the metrics.
  for (auto immIter = instance_metric_map_.begin();
      immIter != instance_metric_map_.end(); ++immIter) {
    sp_string instance_id = immIter->first;
    for (auto iter = instance_info_.begin(); iter != instance_info_.end(); ++iter) {
      if (iter->second->instance_->instance_id() != instance_id) continue;
      InstanceData* data = iter->second;
      Connection* iConn = data->conn_;
      if (!iConn) break;
      sp_string metric_name = MakeBackPressureCompIdMetricName(instance_id);
      metrics_manager_client_->unregister_metric(metric_name);
      delete immIter->second;
    }
  }

  for (auto qmmIter = queue_metric_map_.begin(); qmmIter != queue_metric_map_.end(); ++qmmIter) {
    const sp_string& instance_id = qmmIter->first;
    for (auto iter = instance_info_.begin(); iter != instance_info_.end(); ++iter) {
      if (iter->second->instance_->instance_id() != instance_id) continue;
      InstanceData* data = iter->second;
      Connection* iConn = data->conn_;
      if (!iConn) break;
      sp_string metric_name = MakeQueueSizeCompIdMetricName(instance_id);
      metrics_manager_client_->unregister_metric(metric_name);
      delete qmmIter->second;
    }
  }
  metrics_manager_client_->unregister_metric("__server");
  metrics_manager_client_->unregister_metric(METRIC_TIME_SPENT_BACK_PRESSURE_AGGR);
  metrics_manager_client_->unregister_metric(METRIC_TIME_SPENT_BACK_PRESSURE_INIT);
  delete stmgr_server_metrics_;
  delete back_pressure_metric_aggr_;
  delete back_pressure_metric_initiated_;

  // cleanup the instance info
  for (auto iter = instance_info_.begin(); iter != instance_info_.end(); ++iter) {
    delete iter->second;
  }
}

void StMgrServer::GetInstanceInfo(std::vector<proto::system::Instance*>& _return) {
  for (auto iter = instance_info_.begin(); iter != instance_info_.end(); ++iter) {
    _return.push_back(iter->second->instance_);
  }
}

sp_string StMgrServer::MakeBackPressureCompIdMetricName(const sp_string& instanceid) {
  return METRIC_TIME_SPENT_BACK_PRESSURE_COMPID + instanceid;
}

sp_string StMgrServer::MakeQueueSizeCompIdMetricName(const sp_string& instanceid) {
  return METRIC_QUEUE_SIZE_TO_INSTANCE_COMPID + instanceid;
}

void StMgrServer::HandleNewConnection(Connection* _conn) {
  // There is nothing to be done here. Instead we wait
  // for the register/hello
  LOG(INFO) << "Got new connection " << _conn << " from " << _conn->getIPAddress() << ":"
            << _conn->getPort();
}

void StMgrServer::HandleConnectionClose(Connection* _conn, NetworkErrorCode) {
  LOG(INFO) << "Got connection close of " << _conn << " from " << _conn->getIPAddress() << ":"
            << _conn->getPort();
  // Did the stream manager ever announce back pressure to us
  if (rstmgrs_.find(_conn) != rstmgrs_.end()) {
    // This is a stmgr connection
    sp_string stmgr_id = rstmgrs_[_conn];
    // Did we receive a start back pressure message from this stmgr to
    // begin with?
    if (stmgrs_who_announced_back_pressure_.find(stmgr_id) !=
        stmgrs_who_announced_back_pressure_.end()) {
      stmgrs_who_announced_back_pressure_.erase(stmgr_id);
    }
  } else if (remote_ends_who_caused_back_pressure_.find(GetInstanceName(_conn)) !=
             remote_ends_who_caused_back_pressure_.end()) {
    _conn->unsetCausedBackPressure();
    // Did the instance ever cause back pressure
    remote_ends_who_caused_back_pressure_.erase(GetInstanceName(_conn));
    // This is a instance connection
    heron::common::TimeSpentMetric* instance_metric = instance_metric_map_[GetInstanceName(_conn)];
    instance_metric->Stop();
    if (remote_ends_who_caused_back_pressure_.empty()) {
      SendStopBackPressureToOtherStMgrs();
    }
  }
  // Note: Connections to other stream managers get handled in StmgrClient
  // Now attempt to stop the back pressure
  AttemptStopBackPressureFromSpouts();

  // Now cleanup the data structures
  auto siter = rstmgrs_.find(_conn);
  if (siter != rstmgrs_.end()) {
    LOG(INFO) << "Stmgr " << siter->second << " closed connection";
    stmgrs_.erase(siter->second);
    rstmgrs_.erase(_conn);
  }

  auto iiter = active_instances_.find(_conn);
  if (iiter != active_instances_.end()) {
    sp_int32 task_id = iiter->second;
    CHECK(instance_info_.find(task_id) != instance_info_.end());
    LOG(INFO) << "Instance " << instance_info_[task_id]->instance_->instance_id()
              << " closed connection";
    instance_info_[task_id]->set_connection(NULL);
    active_instances_.erase(_conn);
  }
}

void StMgrServer::HandleStMgrHelloRequest(REQID _id, Connection* _conn,
                                          proto::stmgr::StrMgrHelloRequest* _request) {
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
  delete _request;
}

void StMgrServer::HandleTupleStreamMessage(Connection* _conn,
                                           proto::stmgr::TupleStreamMessage2* _message) {
  auto iter = rstmgrs_.find(_conn);
  if (iter == rstmgrs_.end()) {
    LOG(INFO) << "Recieved Tuple messages from unknown streammanager connection" << std::endl;
  } else {
    stmgr_->HandleStreamManagerData(iter->second, *_message);
  }
  release(_message);
}

void StMgrServer::HandleRegisterInstanceRequest(REQID _reqid, Connection* _conn,
                                                proto::stmgr::RegisterInstanceRequest* _request) {
  LOG(INFO) << "Got HandleRegisterInstanceRequest from connection " << _conn << " and instance "
            << _request->instance().instance_id();
  // Do some basic checks
  bool error = false;
  if (_request->topology_name() != topology_name_ || _request->topology_id() != topology_id_) {
    LOG(ERROR) << "Invalid topology name/id in register instance request"
               << " Found " << _request->topology_name() << " and " << _request->topology_id()
               << std::endl;
    error = true;
  }
  const sp_string& instance_id = _request->instance().instance_id();
  sp_int32 task_id = _request->instance().info().task_id();
  if (!error) {
    bool expected = false;
    for (size_t i = 0; i < expected_instances_.size(); ++i) {
      if (expected_instances_[i] == instance_id) {
        expected = true;
        break;
      }
    }
    if (!expected) {
      LOG(ERROR) << "Unexpected instance in register instance request " << instance_id;
      error = true;
    }
  }

  if (instance_info_.find(task_id) != instance_info_.end() &&
      instance_info_[task_id]->conn_ != NULL) {
    LOG(ERROR) << "Instance with the same task id already exists in our map " << instance_id;

    LOG(ERROR) << "Closing the old connection";
    instance_info_[task_id]->conn_->closeConnection();
    error = true;
  }
  if (error) {
    proto::stmgr::RegisterInstanceResponse response;
    response.mutable_status()->set_status(proto::system::NOTOK);
    SendResponse(_reqid, _conn, response);
  } else {
    LOG(INFO) << "New instance registered with us " << instance_id;
    active_instances_[_conn] = task_id;
    if (instance_info_.find(task_id) == instance_info_.end()) {
      instance_info_[task_id] = new InstanceData(_request->release_instance());
      // Create a metric for this instance
      if (instance_metric_map_.find(instance_id) == instance_metric_map_.end()) {
        auto instance_metric = new heron::common::TimeSpentMetric();
        metrics_manager_client_->register_metric(MakeBackPressureCompIdMetricName(instance_id),
                                                 instance_metric);
        instance_metric_map_[instance_id] = instance_metric;
      }
      if (queue_metric_map_.find(instance_id) == queue_metric_map_.end()) {
        auto queue_metric = new heron::common::AssignableMetric(0);
        metrics_manager_client_->register_metric(MakeQueueSizeCompIdMetricName(instance_id),
                                                 queue_metric);
        queue_metric_map_[instance_id] = queue_metric;
      }
    }
    instance_info_[task_id]->set_connection(_conn);

    proto::stmgr::RegisterInstanceResponse response;
    response.mutable_status()->set_status(proto::system::OK);
    const proto::system::PhysicalPlan* pplan = stmgr_->GetPhysicalPlan();
    if (pplan) {
      response.mutable_pplan()->CopyFrom(*pplan);
    }
    SendResponse(_reqid, _conn, response);

    // Have all the instances connected to us?
    if (HaveAllInstancesConnectedToUs()) {
      // Now we can connect to the tmaster
      stmgr_->StartTMasterClient();
    }
  }
  delete _request;
}

void StMgrServer::HandleTupleSetMessage(Connection* _conn,
                                        proto::system::HeronTupleSet* _message) {
  auto iter = active_instances_.find(_conn);
  if (iter == active_instances_.end()) {
    LOG(ERROR) << "Received TupleSet from unknown instance connection. Dropping.." << std::endl;
    release(_message);
    return;
  }
  if (_message->has_data()) {
    stmgr_server_metrics_->scope(METRIC_DATA_TUPLES_FROM_INSTANCES)
        ->incr_by(_message->data().tuples_size());
  } else if (_message->has_control()) {
    stmgr_server_metrics_->scope(METRIC_ACK_TUPLES_FROM_INSTANCES)
        ->incr_by(_message->control().acks_size());
    stmgr_server_metrics_->scope(METRIC_FAIL_TUPLES_FROM_INSTANCES)
        ->incr_by(_message->control().fails_size());
  }
  stmgr_->HandleInstanceData(iter->second, instance_info_[iter->second]->local_spout_, _message);
  release(_message);
}

void StMgrServer::SendToInstance2(sp_int32 _task_id,
                                  sp_int32 _byte_size,
                                  const sp_string _type_name,
                                  const char* _message) {
  bool drop = false;
  TaskIdInstanceDataMap::iterator iter = instance_info_.find(_task_id);
  if (iter == instance_info_.end() || iter->second->conn_ == NULL) {
    LOG(ERROR) << "task_id " << _task_id << " has not yet connected to us. Dropping..."
               << std::endl;
    drop = true;
  }

  if (drop) {
  } else {
    SendMessage(iter->second->conn_, _byte_size, _type_name, _message);
  }
}

void StMgrServer::SendToInstance2(sp_int32 _task_id,
                                  const proto::system::HeronTupleSet2& _message) {
  bool drop = false;
  TaskIdInstanceDataMap::iterator iter = instance_info_.find(_task_id);
  if (iter == instance_info_.end() || iter->second->conn_ == NULL) {
    LOG(ERROR) << "task_id " << _task_id << " has not yet connected to us. Dropping..."
               << std::endl;
    drop = true;
  }
  if (drop) {
    if (_message.has_control()) {
      stmgr_server_metrics_->scope(METRIC_ACK_TUPLES_TO_INSTANCES_LOST)
          ->incr_by(_message.control().acks_size());
      stmgr_server_metrics_->scope(METRIC_FAIL_TUPLES_TO_INSTANCES_LOST)
          ->incr_by(_message.control().fails_size());
    }
  } else {
    if (_message.has_control()) {
      stmgr_server_metrics_->scope(METRIC_ACK_TUPLES_TO_INSTANCES)
          ->incr_by(_message.control().acks_size());
      stmgr_server_metrics_->scope(METRIC_FAIL_TUPLES_TO_INSTANCES)
          ->incr_by(_message.control().fails_size());
    }
    SendMessage(iter->second->conn_, _message);
  }
}

void StMgrServer::BroadcastNewPhysicalPlan(const proto::system::PhysicalPlan& _pplan) {
  // TODO(vikasr) We do not handle any changes to our local assignment
  ComputeLocalSpouts(_pplan);
  proto::stmgr::NewInstanceAssignmentMessage new_assignment;
  new_assignment.mutable_pplan()->CopyFrom(_pplan);
  for (auto iter = active_instances_.begin(); iter != active_instances_.end(); ++iter) {
    SendMessage(iter->first, new_assignment);
  }
}

void StMgrServer::ComputeLocalSpouts(const proto::system::PhysicalPlan& _pplan) {
  std::set<sp_int32> local_spouts;
  config::PhysicalPlanHelper::GetLocalSpouts(_pplan, stmgr_id_, local_spouts);
  for (auto iter = instance_info_.begin(); iter != instance_info_.end(); ++iter) {
    if (local_spouts.find(iter->first) != local_spouts.end()) {
      iter->second->set_local_spout();
    }
  }
}

sp_string StMgrServer::GetInstanceName(Connection* _connection) {
  // Indicate which instance component had back pressure
  auto itr = active_instances_.find(_connection);
  if (itr != active_instances_.end()) {
    sp_int32 task_id = itr->second;
    const sp_string& instance_id = instance_info_[task_id]->instance_->instance_id();
    return instance_id;
  }
  return "";
}

void StMgrServer::StartBackPressureConnectionCb(Connection* _connection) {
  // The connection will notify us when we can stop the back pressure
  _connection->setCausedBackPressure();

  // Find the instance this connection belongs to
  sp_string instance_name = GetInstanceName(_connection);
  CHECK_NE(instance_name, "");

  if (remote_ends_who_caused_back_pressure_.empty()) {
    SendStartBackPressureToOtherStMgrs();
    back_pressure_metric_initiated_->Start();
  }

  // Indicate which instance component had back pressure
  heron::common::TimeSpentMetric* instance_metric = instance_metric_map_[instance_name];
  instance_metric->Start();

  remote_ends_who_caused_back_pressure_.insert(instance_name);
  LOG(INFO) << "We observe back pressure on sending data to instance " << instance_name;
  StartBackPressureOnSpouts();
}

void StMgrServer::StopBackPressureConnectionCb(Connection* _connection) {
  _connection->unsetCausedBackPressure();

  // Find the instance this connection belongs to
  sp_string instance_name = GetInstanceName(_connection);
  CHECK_NE(instance_name, "");

  CHECK(remote_ends_who_caused_back_pressure_.find(instance_name) !=
        remote_ends_who_caused_back_pressure_.end());
  remote_ends_who_caused_back_pressure_.erase(instance_name);

  // Indicate which instance component stopped back pressure
  heron::common::TimeSpentMetric* instance_metric = instance_metric_map_[instance_name];
  instance_metric->Stop();

  if (remote_ends_who_caused_back_pressure_.empty()) {
    SendStopBackPressureToOtherStMgrs();
    back_pressure_metric_initiated_->Stop();
  }
  LOG(INFO) << "We don't observe back pressure now on sending data to instance " << instance_name;
  AttemptStopBackPressureFromSpouts();
}

void StMgrServer::ConnectionBufferChangeCb(Connection* _connection) {
  // Find the instance this connection belongs to
  const sp_string& instance_name = GetInstanceName(_connection);
  if (instance_name != "") {
    sp_int32 bytes = _connection->getOutstandingBytes();
    queue_metric_map_[instance_name]->SetValue(bytes);
  }
}

void StMgrServer::StartBackPressureClientCb(const sp_string& _other_stmgr_id) {
  if (remote_ends_who_caused_back_pressure_.empty()) {
    SendStartBackPressureToOtherStMgrs();
    back_pressure_metric_initiated_->Start();
  }
  remote_ends_who_caused_back_pressure_.insert(_other_stmgr_id);
  LOG(INFO) << "We observe back pressure on sending data to remote stream manager "
            << _other_stmgr_id;
  StartBackPressureOnSpouts();
}

void StMgrServer::StopBackPressureClientCb(const sp_string& _other_stmgr_id) {
  CHECK(remote_ends_who_caused_back_pressure_.find(_other_stmgr_id) !=
        remote_ends_who_caused_back_pressure_.end());
  remote_ends_who_caused_back_pressure_.erase(_other_stmgr_id);

  if (remote_ends_who_caused_back_pressure_.empty()) {
    SendStopBackPressureToOtherStMgrs();
    back_pressure_metric_initiated_->Stop();
  }
  LOG(INFO) << "We don't observe back pressure now on sending data to remote "
               "stream manager "
            << _other_stmgr_id;
  AttemptStopBackPressureFromSpouts();
}

void StMgrServer::HandleStartBackPressureMessage(Connection* _conn,
                                                 proto::stmgr::StartBackPressureMessage* _message) {
  // Close spouts
  LOG(INFO) << "Received start back pressure from str mgr " << _message->stmgr();
  if (_message->topology_name() != topology_name_ || _message->topology_id() != topology_id_) {
    LOG(ERROR) << "Received start back pressure message from unknown stream manager "
               << _message->topology_name() << " " << _message->topology_id() << " "
               << _message->stmgr() << " " << _message->message_id();

    release(_message);
    return;
  }
  auto iter = rstmgrs_.find(_conn);
  CHECK(iter != rstmgrs_.end());
  sp_string stmgr_id = iter->second;
  stmgrs_who_announced_back_pressure_.insert(stmgr_id);

  StartBackPressureOnSpouts();

  release(_message);
}

void StMgrServer::HandleStopBackPressureMessage(Connection* _conn,
                                                proto::stmgr::StopBackPressureMessage* _message) {
  LOG(INFO) << "Received stop back pressure from str mgr " << _message->stmgr();
  if (_message->topology_name() != topology_name_ || _message->topology_id() != topology_id_) {
    LOG(ERROR) << "Received stop back pressure message from unknown stream manager "
               << _message->topology_name() << " " << _message->topology_id() << " "
               << _message->stmgr();

    release(_message);
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
    AttemptStopBackPressureFromSpouts();
  }

  release(_message);
}

void StMgrServer::SendStartBackPressureToOtherStMgrs() {
  LOG(INFO) << "Sending start back pressure notification to all other "
            << "stream managers";
  stmgr_->SendStartBackPressureToOtherStMgrs();
}

void StMgrServer::SendStopBackPressureToOtherStMgrs() {
  LOG(INFO) << "Sending stop back pressure notification to all other "
            << "stream managers";
  stmgr_->SendStopBackPressureToOtherStMgrs();
}

void StMgrServer::StartBackPressureOnSpouts() {
  if (!spouts_under_back_pressure_) {
    LOG(WARNING) << "Stopping reading from spouts to do back pressure";

    spouts_under_back_pressure_ = true;
    // Put back pressure on all spouts
    for (auto iiter = instance_info_.begin(); iiter != instance_info_.end(); ++iiter) {
      if (!iiter->second->local_spout_) continue;
      if (!iiter->second->conn_) continue;
      if (!iiter->second->conn_->isUnderBackPressure()) iiter->second->conn_->putBackPressure();
    }
    back_pressure_metric_aggr_->Start();
  }
}

void StMgrServer::AttemptStopBackPressureFromSpouts() {
  if (spouts_under_back_pressure_ && remote_ends_who_caused_back_pressure_.empty() &&
      stmgrs_who_announced_back_pressure_.empty()) {
    LOG(INFO) << "Starting reading from spouts to relieve back pressure";
    spouts_under_back_pressure_ = false;

    // Remove backpressure from all pipes
    for (auto iiter = instance_info_.begin(); iiter != instance_info_.end(); ++iiter) {
      if (!iiter->second->local_spout_) continue;
      if (!iiter->second->conn_) continue;
      if (iiter->second->conn_->isUnderBackPressure()) iiter->second->conn_->removeBackPressure();
    }
    back_pressure_metric_aggr_->Stop();
  }
}

}  // namespace stmgr
}  // namespace heron
