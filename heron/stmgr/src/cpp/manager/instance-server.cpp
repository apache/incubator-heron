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

#include "manager/instance-server.h"

#include <iostream>
#include <map>
#include <string>
#include <unordered_set>
#include <vector>
#include "manager/checkpoint-gateway.h"
#include "util/neighbour-calculator.h"
#include "manager/stmgr.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/helper.h"
#include "config/heron-internals-config-reader.h"
#include "config/topology-config-helper.h"
#include "metrics/metrics.h"

namespace heron {
namespace stmgr {

using std::make_shared;

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

// Time spent in back pressure caused by instances managed by this stmgr.
const sp_string METRIC_TIME_SPENT_BACK_PRESSURE_CAUSED_BY_LOCAL_INSTANCE =
    "__time_spent_back_pressure_by_local_instance";
// Time spent in back pressure aggregated - back pressure initiated by us +
// others
const sp_string METRIC_TIME_SPENT_BACK_PRESSURE_AGGR = "__server/__time_spent_back_pressure_aggr";
// Time spent in back pressure because of a component id. The comp id will be
// appended to the string below
const sp_string METRIC_TIME_SPENT_BACK_PRESSURE_COMPID = "__time_spent_back_pressure_by_compid/";

// Prefix for connection buffer's metrics
const sp_string CONNECTION_BUFFER_BY_INSTANCEID = "__connection_buffer_by_instanceid/";
// Prefix for connection buffer's length metrics. This is different
// from METRIC_DATA_TUPLES_TO_INSTANCES as that counts
// the tuples when they are sent to the instance -- this metric
// will be used to count the tuples as they are received
const sp_string CONNECTION_BUFFER_LENGTH_BY_INSTANCEID =
  "__connection_buffer_length_by_instanceid/";

// TODO(mfu): Read this value from config
const sp_int64 SYSTEM_METRICS_SAMPLE_INTERVAL_MICROSECOND = 10_s;


InstanceServer::InstanceServer(
    shared_ptr<EventLoop> eventLoop,
    const NetworkOptions& _options,
    const sp_string& _topology_name, const sp_string& _topology_id,
    const sp_string& _stmgr_id,
    const std::vector<sp_string>& _expected_instances,
    StMgr* _stmgr,
    shared_ptr<heron::common::MetricsMgrSt> const& _metrics_manager_client,
    shared_ptr<NeighbourCalculator> _neighbour_calculator,
    bool _droptuples_upon_backpressure)
    : Server(eventLoop, _options),
      topology_name_(_topology_name),
      topology_id_(_topology_id),
      stmgr_id_(_stmgr_id),
      expected_instances_(_expected_instances),
      stmgr_(_stmgr),
      metrics_manager_client_(_metrics_manager_client),
      neighbour_calculator_(_neighbour_calculator),
      droptuples_upon_backpressure_(_droptuples_upon_backpressure) {
  // instance related handlers
  InstallRequestHandler(&InstanceServer::HandleRegisterInstanceRequest);
  InstallMessageHandler(&InstanceServer::HandleTupleSetMessage);
  InstallMessageHandler(&InstanceServer::HandleStoreInstanceStateCheckpointMessage);
  InstallMessageHandler(&InstanceServer::HandleRestoreInstanceStateResponse);

  instance_server_metrics_ = make_shared<heron::common::MultiCountMetric>();
  back_pressure_metric_aggr_ = make_shared<heron::common::TimeSpentMetric>();
  metrics_manager_client_->register_metric("__server", instance_server_metrics_);
  metrics_manager_client_->register_metric(METRIC_TIME_SPENT_BACK_PRESSURE_AGGR,
                                           back_pressure_metric_aggr_);
  back_pressure_metric_caused_by_local_instances_ = make_shared<heron::common::TimeSpentMetric>();
  metrics_manager_client_->register_metric(METRIC_TIME_SPENT_BACK_PRESSURE_CAUSED_BY_LOCAL_INSTANCE,
                                           back_pressure_metric_caused_by_local_instances_);
  spouts_under_back_pressure_ = false;

  // Update queue related metrics every 10 seconds
  CHECK_GT(eventLoop_->registerTimer([this](EventLoop::Status status) {
    this->UpdateQueueMetrics(status);
  }, true, SYSTEM_METRICS_SAMPLE_INTERVAL_MICROSECOND), 0);

  sp_uint64 drain_threshold_bytes =
    config::HeronInternalsConfigReader::Instance()->GetHeronStreammgrStatefulBufferSizeMb() * 1_MB;
  stateful_gateway_ = make_shared<CheckpointGateway>(drain_threshold_bytes, neighbour_calculator_,
                                            metrics_manager_client_,
    std::bind(&InstanceServer::DrainTupleSet, this, std::placeholders::_1, std::placeholders::_2),
    std::bind(&InstanceServer::DrainTupleStream, this, std::placeholders::_1),
    std::bind(&InstanceServer::DrainCheckpoint, this, std::placeholders::_1,
              std::placeholders::_2));
}

InstanceServer::~InstanceServer() {
  Stop();
  // Unregister and delete the metrics.
  for (auto immIter = instance_metric_map_.begin();
            immIter != instance_metric_map_.end();
            immIter = instance_metric_map_.erase(immIter)) {
    const sp_string& instance_id = immIter->first;

    for (auto iter = instance_info_.begin(); iter != instance_info_.end(); ++iter) {
      if (iter->second->instance_->instance_id() != instance_id) continue;
      InstanceData* data = iter->second;
      Connection* iConn = data->conn_;
      if (!iConn) break;
      sp_string metric_name = MakeBackPressureCompIdMetricName(instance_id);
      metrics_manager_client_->unregister_metric(metric_name);
    }
  }

  // Clean the connection_buffer_metric_map
  for (auto qmmIter = connection_buffer_metric_map_.begin();
            qmmIter != connection_buffer_metric_map_.end();) {
    const sp_string& instance_id = qmmIter->first;

    for (auto iter = instance_info_.begin(); iter != instance_info_.end(); ++iter) {
      if (iter->second->instance_->instance_id() != instance_id) continue;
      InstanceData* data = iter->second;
      Connection* iConn = data->conn_;
      if (!iConn) break;
      sp_string metric_name = MakeQueueSizeCompIdMetricName(instance_id);
      metrics_manager_client_->unregister_metric(metric_name);
    }

    qmmIter = connection_buffer_metric_map_.erase(qmmIter);
  }

  // Clean the connection_buffer_length_metric_map
  for (auto qlmIter = connection_buffer_length_metric_map_.begin();
            qlmIter != connection_buffer_length_metric_map_.end();) {
    const sp_string& instance_id = qlmIter->first;
    sp_string metric_name = MakeQueueLengthCompIdMetricName(instance_id);
    metrics_manager_client_->unregister_metric(metric_name);
    qlmIter = connection_buffer_length_metric_map_.erase(qlmIter);
  }

  metrics_manager_client_->unregister_metric("__server");
  metrics_manager_client_->unregister_metric(METRIC_TIME_SPENT_BACK_PRESSURE_AGGR);
  metrics_manager_client_->unregister_metric(
      METRIC_TIME_SPENT_BACK_PRESSURE_CAUSED_BY_LOCAL_INSTANCE);

  // cleanup the instance info
  for (auto iter = instance_info_.begin(); iter != instance_info_.end(); ++iter) {
    delete iter->second;
  }
}

void InstanceServer::GetInstanceInfo(std::vector<proto::system::Instance*>& _return) {
  for (auto iter = instance_info_.begin(); iter != instance_info_.end(); ++iter) {
    _return.push_back(iter->second->instance_);
  }
}

proto::system::Instance* InstanceServer::GetInstanceInfo(sp_int32 _task_id) {
  auto iter = instance_info_.find(_task_id);
  if (iter == instance_info_.end()) {
    return NULL;
  } else {
    return iter->second->instance_;
  }
}

sp_string InstanceServer::MakeBackPressureCompIdMetricName(const sp_string& instanceid) {
  return METRIC_TIME_SPENT_BACK_PRESSURE_COMPID + instanceid;
}

sp_string InstanceServer::MakeQueueSizeCompIdMetricName(const sp_string& instanceid) {
  return CONNECTION_BUFFER_BY_INSTANCEID + instanceid;
}

sp_string InstanceServer::MakeQueueLengthCompIdMetricName(const sp_string& instanceid) {
  return CONNECTION_BUFFER_LENGTH_BY_INSTANCEID + instanceid;
}

void InstanceServer::UpdateQueueMetrics(EventLoop::Status) {
  for (auto itr = active_instances_.begin(); itr != active_instances_.end(); ++itr) {
    sp_int32 task_id = itr->second;
    const sp_string& instance_id = instance_info_[task_id]->instance_->instance_id();
    sp_int32 bytes = itr->first->getOutstandingBytes();
    connection_buffer_metric_map_[instance_id]->scope("bytes")->record(bytes);
  }
}

void InstanceServer::HandleNewConnection(Connection* _conn) {
  // There is nothing to be done here. Instead we wait
  // for the register/hello
  LOG(INFO) << "Instance Server got new connection " << _conn << " from "
            << _conn->getIPAddress() << ":" << _conn->getPort();
}

void InstanceServer::HandleConnectionClose(Connection* _conn, NetworkErrorCode) {
  LOG(INFO) << "Instance Server got connection close of " << _conn << " from "
            << _conn->getIPAddress() << ":" << _conn->getPort();
  // Did the instance ever announce back pressure to us
  if (remote_ends_who_caused_back_pressure_.find(GetInstanceName(_conn)) !=
      remote_ends_who_caused_back_pressure_.end()) {
    _conn->unsetCausedBackPressure();
    remote_ends_who_caused_back_pressure_.erase(GetInstanceName(_conn));
    auto instance_metric = instance_metric_map_[GetInstanceName(_conn)];
    instance_metric->Stop();
    if (!stmgr_->DidAnnounceBackPressure()) {
      stmgr_->SendStopBackPressureToOtherStMgrs();
    }
  }
  // Note: Connections to other stream managers get handled in StmgrClient
  // Now attempt to stop the back pressure
  AttemptStopBackPressureFromSpouts();

  auto iiter = active_instances_.find(_conn);
  if (iiter != active_instances_.end()) {
    sp_int32 task_id = iiter->second;
    CHECK(instance_info_.find(task_id) != instance_info_.end());
    sp_string instance_id = instance_info_[task_id]->instance_->instance_id();
    LOG(INFO) << "Instance " << instance_id << " closed connection";

    // Remove the connection from active instances
    active_instances_.erase(_conn);

    // Set the connection to null. Do not delete the structure
    instance_info_[task_id]->set_connection(NULL);

    // Clean the instance_metric_map
    auto immiter = instance_metric_map_.find(instance_id);
    if (immiter != instance_metric_map_.end()) {
      metrics_manager_client_->unregister_metric(MakeBackPressureCompIdMetricName(instance_id));
      instance_metric_map_.erase(instance_id);
    }

    // Clean the connection_buffer_metric_map_
    auto qmmiter = connection_buffer_metric_map_.find(instance_id);
    if (qmmiter != connection_buffer_metric_map_.end()) {
      metrics_manager_client_->unregister_metric(MakeQueueSizeCompIdMetricName(instance_id));
      connection_buffer_metric_map_.erase(instance_id);
    }

    // Clean the connection_buffer_length_metric_map_
    auto qlmiter = connection_buffer_length_metric_map_.find(instance_id);
    if (qlmiter != connection_buffer_length_metric_map_.end()) {
      metrics_manager_client_->unregister_metric(MakeQueueLengthCompIdMetricName(instance_id));
      connection_buffer_length_metric_map_.erase(instance_id);
    }

    stmgr_->HandleDeadInstance(task_id);
  }
}

void InstanceServer::HandleRegisterInstanceRequest(REQID _reqid, Connection* _conn,
                                  pool_unique_ptr<proto::stmgr::RegisterInstanceRequest> _request) {
  LOG(INFO) << "Got HandleRegisterInstanceRequest from connection " << _conn << " and instance "
            << _request->instance().instance_id();
  // Do some basic checks
  bool error = false;
  if (_request->topology_name() != topology_name_ || _request->topology_id() != topology_id_) {
    LOG(ERROR) << "Invalid topology name/id in register instance request"
               << " Found " << _request->topology_name() << " and " << _request->topology_id();
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
    }
    // Create a metric for this instance
    if (instance_metric_map_.find(instance_id) == instance_metric_map_.end()) {
      auto instance_metric = make_shared<heron::common::TimeSpentMetric>();
      metrics_manager_client_->register_metric(MakeBackPressureCompIdMetricName(instance_id),
                                               instance_metric);
      instance_metric_map_[instance_id] = instance_metric;
    }
    if (connection_buffer_metric_map_.find(instance_id) == connection_buffer_metric_map_.end()) {
      auto queue_metric = make_shared<heron::common::MultiMeanMetric>();
      metrics_manager_client_->register_metric(MakeQueueSizeCompIdMetricName(instance_id),
                                               queue_metric);
      connection_buffer_metric_map_[instance_id] = queue_metric;
    }

    if (connection_buffer_length_metric_map_.find(instance_id) ==
      connection_buffer_length_metric_map_.end()) {
      task_id_to_name[task_id] = instance_id;
      auto queue_metric = make_shared<heron::common::MultiCountMetric>();
      metrics_manager_client_->register_metric(MakeQueueLengthCompIdMetricName(instance_id),
                                               queue_metric);
      connection_buffer_length_metric_map_[instance_id] = queue_metric;
    }
    instance_info_[task_id]->set_connection(_conn);

    proto::stmgr::RegisterInstanceResponse response;
    response.mutable_status()->set_status(proto::system::OK);
    const shared_ptr<proto::system::PhysicalPlan> pplan = stmgr_->GetPhysicalPlan();
    if (pplan) {
      response.mutable_pplan()->CopyFrom(*pplan);
    }
    SendResponse(_reqid, _conn, response);
    // Apply rate limit to the connection
    if (pplan) {
      const std::string component = instance_info_[task_id]->instance_->info().component_name();
      SetRateLimit(*pplan, component, _conn);
    }

    // Have all the instances connected to us?
    if (HaveAllInstancesConnectedToUs()) {
      // Notify to stmgr so that it might want to connect to tmaster
      stmgr_->HandleAllInstancesConnected();
    }
  }
}

void InstanceServer::HandleTupleSetMessage(Connection* _conn,
                                           pool_unique_ptr<proto::system::HeronTupleSet> _message) {
  auto iter = active_instances_.find(_conn);
  if (iter == active_instances_.end()) {
    LOG(ERROR) << "Received TupleSet from unknown instance connection. Dropping..";
    return;
  }

  if (_message->has_data()) {
    instance_server_metrics_->scope(METRIC_DATA_TUPLES_FROM_INSTANCES)
        ->incr_by(_message->data().tuples_size());
  } else if (_message->has_control()) {
    instance_server_metrics_->scope(METRIC_ACK_TUPLES_FROM_INSTANCES)
        ->incr_by(_message->control().acks_size());
    instance_server_metrics_->scope(METRIC_FAIL_TUPLES_FROM_INSTANCES)
        ->incr_by(_message->control().fails_size());
  }

  stmgr_->HandleInstanceData(iter->second, instance_info_[iter->second]->local_spout_,
                             std::move(_message));
}

void InstanceServer::SendToInstance2(pool_unique_ptr<proto::stmgr::TupleStreamMessage> _message) {
  sp_string instance_id = task_id_to_name[_message->task_id()];
  ConnectionBufferLengthMetricMap::const_iterator it =
    connection_buffer_length_metric_map_.find(instance_id);
  if ( it != connection_buffer_length_metric_map_.end() )
    connection_buffer_length_metric_map_
      [instance_id]->scope("packets")->incr_by(_message->num_tuples());

  stateful_gateway_->SendToInstance(std::move(_message));
}

void InstanceServer::DrainTupleStream(proto::stmgr::TupleStreamMessage* _message) {
  sp_int32 task_id = _message->task_id();
  TaskIdInstanceDataMap::iterator iter = instance_info_.find(task_id);
  if (iter == instance_info_.end() || iter->second->conn_ == NULL) {
    LOG_EVERY_N(ERROR, 100) << "task_id " << task_id << " has not yet connected to us. Dropping...";
  } else {
    SendMessage(iter->second->conn_, _message->set().size(),
                heron_tuple_set_2_, _message->set().c_str());
  }
  __global_protobuf_pool_release__(_message);
}

void InstanceServer::SendToInstance2(sp_int32 _task_id,
                                  proto::system::HeronTupleSet2* _message) {
  if (_message->has_data()) {
    sp_string instance_id = task_id_to_name[_task_id];

    ConnectionBufferLengthMetricMap::const_iterator it =
      connection_buffer_length_metric_map_.find(instance_id);

    if ( it != connection_buffer_length_metric_map_.end() )
      connection_buffer_length_metric_map_
        [instance_id]->scope("packets")->incr_by(_message->data().tuples_size());
  }
  stateful_gateway_->SendToInstance(_task_id, _message);
}

void InstanceServer::DrainTupleSet(sp_int32 _task_id,
                                proto::system::HeronTupleSet2* _message) {
  TaskIdInstanceDataMap::iterator iter = instance_info_.find(_task_id);
  if (iter == instance_info_.end() || iter->second->conn_ == NULL
      || (droptuples_upon_backpressure_ && iter->second->conn_->hasCausedBackPressure())) {
    LOG_EVERY_N(ERROR, 100) << "task_id " << _task_id << " has not yet connected to us or is not "
                            << "keeping up and droptuples_upon_backpressure is set to true. "
                            << "Dropping...";
    if (_message->has_data()) {
      instance_server_metrics_->scope(METRIC_DATA_TUPLES_TO_INSTANCES_LOST)
          ->incr_by(_message->data().tuples_size());
    } else if (_message->has_control()) {
      instance_server_metrics_->scope(METRIC_ACK_TUPLES_TO_INSTANCES_LOST)
          ->incr_by(_message->control().acks_size());
      instance_server_metrics_->scope(METRIC_FAIL_TUPLES_TO_INSTANCES_LOST)
          ->incr_by(_message->control().fails_size());
    }
  } else {
    if (_message->has_data()) {
      instance_server_metrics_->scope(METRIC_DATA_TUPLES_TO_INSTANCES)
          ->incr_by(_message->data().tuples_size());
    } else if (_message->has_control()) {
      instance_server_metrics_->scope(METRIC_ACK_TUPLES_TO_INSTANCES)
          ->incr_by(_message->control().acks_size());
      instance_server_metrics_->scope(METRIC_FAIL_TUPLES_TO_INSTANCES)
          ->incr_by(_message->control().fails_size());
    }
    SendMessage(iter->second->conn_, *_message);
  }
  __global_protobuf_pool_release__(_message);
}

void InstanceServer::DrainCheckpoint(sp_int32 _task_id,
                             pool_unique_ptr<proto::ckptmgr::InitiateStatefulCheckpoint> _message) {
  TaskIdInstanceDataMap::iterator iter = instance_info_.find(_task_id);
  if (iter == instance_info_.end() || iter->second->conn_ == NULL) {
    LOG(ERROR) << "task_id " << _task_id << " has not yet connected to us. Dropping...";
  } else {
    LOG(INFO) << "Sending Initiate Checkpoint Message to local task " << _task_id;
    SendMessage(iter->second->conn_, *_message);
  }
}

void InstanceServer::BroadcastNewPhysicalPlan(const proto::system::PhysicalPlan& _pplan) {
  // TODO(vikasr) We do not handle any changes to our local assignment
  LOG(INFO) << "Broadcasting new PhysicalPlan:";
  config::TopologyConfigHelper::LogTopology(_pplan.topology());

  ComputeLocalSpouts(_pplan);
  proto::stmgr::NewInstanceAssignmentMessage new_assignment;
  new_assignment.mutable_pplan()->CopyFrom(_pplan);
  for (auto iter = active_instances_.begin(); iter != active_instances_.end(); ++iter) {
    LOG(INFO) << "Sending new physical plan to instance with task_id: " << iter->second;
    Connection* conn = iter->first;
    SendMessage(conn, new_assignment);
    // Update connection's rate limiting base on component config
    sp_int32 id = iter->second;
    const std::string component = instance_info_[id]->instance_->info().component_name();
    SetRateLimit(_pplan, component, conn);
  }
}

void InstanceServer::BroadcastStatefulCheckpointSaved(
    const proto::ckptmgr::StatefulConsistentCheckpointSaved& _msg) {
  for (auto & iter : active_instances_) {
    LOG(INFO) << "Sending checkpoint: " << _msg.consistent_checkpoint().checkpoint_id()
              << " saved message to instance with task_id: " << iter.second;
    SendMessage(iter.first, _msg);
  }
}

void InstanceServer::SetRateLimit(const proto::system::PhysicalPlan& _pplan,
                                  const std::string& _component,
                                  Connection* _conn) const {
  sp_int64 read_bps =
      config::TopologyConfigHelper::GetComponentOutputBPS(_pplan.topology(), _component);
  sp_int32 parallelism =
      config::TopologyConfigHelper::GetComponentParallelism(_pplan.topology(), _component);
  // burst rate is 1.5 x of regular rate
  sp_int64 burst_read_bps = read_bps + read_bps / 2;

  LOG(INFO) << "Parallelism of component " << _component << " is " << parallelism;
  LOG(INFO) << "Read BPS of component " << _component << " is " << read_bps;
  if (parallelism > 0 && read_bps >= 0 && burst_read_bps >= 0) {
    LOG(INFO) << "Set rate limit in " << _component << " to " << read_bps << "/" << burst_read_bps;
    _conn->setRateLimit(read_bps / parallelism, burst_read_bps / parallelism);
  } else {
    LOG(INFO) << "Disable rate limit in " << _component;
    _conn->disableRateLimit();
  }
}

void InstanceServer::ComputeLocalSpouts(const proto::system::PhysicalPlan& _pplan) {
  std::unordered_set<sp_int32> local_spouts;
  config::PhysicalPlanHelper::GetLocalSpouts(_pplan, stmgr_id_, local_spouts);
  for (auto iter = instance_info_.begin(); iter != instance_info_.end(); ++iter) {
    if (local_spouts.find(iter->first) != local_spouts.end()) {
      iter->second->set_local_spout();
    }
  }
}

sp_string InstanceServer::GetInstanceName(Connection* _connection) {
  // Indicate which instance component had back pressure
  auto itr = active_instances_.find(_connection);
  if (itr != active_instances_.end()) {
    sp_int32 task_id = itr->second;
    const sp_string& instance_id = instance_info_[task_id]->instance_->instance_id();
    return instance_id;
  }
  return "";
}

// This function is called when the buffer in the connection is full (the instance is not consuming
// tuples fast enough).
void InstanceServer::StartBackPressureConnectionCb(Connection* _connection) {
  // The connection will notify us when we can stop the back pressure
  _connection->setCausedBackPressure();

  // Find the instance this connection belongs to
  sp_string instance_name = GetInstanceName(_connection);
  CHECK_NE(instance_name, "");

  LOG(WARNING) << "We observe back pressure on sending data to instance " << instance_name;

  if (droptuples_upon_backpressure_) {
    // We will drop further tuples to this instance until the backpressure lets off
    LOG(WARNING) << "Backpressure mechanism not initiated since droptuples_upon_backpressure "
                 << "is set";
    return;
  }

  if (!stmgr_->DidAnnounceBackPressure()) {
    stmgr_->SendStartBackPressureToOtherStMgrs();
    // Start backpressure from local instances metric
    back_pressure_metric_caused_by_local_instances_->Start();
  }

  // Indicate which instance component had back pressure
  auto instance_metric = instance_metric_map_[instance_name];
  instance_metric->Start();

  remote_ends_who_caused_back_pressure_.insert(instance_name);
  StartBackPressureOnSpouts();
}

// This function is called when the buffer in the connection is empty (the tuples are drained).
void InstanceServer::StopBackPressureConnectionCb(Connection* _connection) {
  _connection->unsetCausedBackPressure();

  // Find the instance this connection belongs to
  sp_string instance_name = GetInstanceName(_connection);
  CHECK_NE(instance_name, "");

  LOG(WARNING) << "We don't observe back pressure now on sending data to instance "
               << instance_name;

  if (droptuples_upon_backpressure_) {
    // We had not initiated any kind of backpressure mechanism in this mode.
    LOG(WARNING) << "Backpressure mechanism not initiated since droptuples_upon_backpressure "
                     << "is set";
    return;
  }

  CHECK(remote_ends_who_caused_back_pressure_.find(instance_name) !=
        remote_ends_who_caused_back_pressure_.end());
  remote_ends_who_caused_back_pressure_.erase(instance_name);

  // Indicate which instance component stopped back pressure
  auto instance_metric = instance_metric_map_[instance_name];
  instance_metric->Stop();

  if (!stmgr_->DidAnnounceBackPressure()) {
    stmgr_->SendStopBackPressureToOtherStMgrs();
    // Stop backpressure from local instances metric
    back_pressure_metric_caused_by_local_instances_->Stop();
  }
  AttemptStopBackPressureFromSpouts();
}

void InstanceServer::StartBackPressureOnSpouts() {
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

void InstanceServer::AttemptStopBackPressureFromSpouts() {
  if (spouts_under_back_pressure_ && !stmgr_->DidAnnounceBackPressure() &&
      !stmgr_->DidOthersAnnounceBackPressure()) {
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

void InstanceServer::InitiateStatefulCheckpoint(const sp_string& _checkpoint_tag) {
  for (auto iter = instance_info_.begin(); iter != instance_info_.end(); ++iter) {
    // Checkpoint markers originate from spouts.
    if (iter->second->is_local_spout() && iter->second->conn_) {
      LOG(INFO) << "Propagating Initiate Stateful Checkpoint for "
                << _checkpoint_tag << " to local spout "
                << iter->second->instance_->info().component_name()
                << " with task_id " << iter->second->instance_->info().task_id();
      auto message = make_unique<proto::ckptmgr::InitiateStatefulCheckpoint>();
      message->set_checkpoint_id(_checkpoint_tag);
      SendMessage(iter->second->conn_, *message);
    }
  }
}

void InstanceServer::HandleStoreInstanceStateCheckpointMessage(Connection* _conn,
                          pool_unique_ptr<proto::ckptmgr::StoreInstanceStateCheckpoint> _message) {
  ConnectionTaskIdMap::iterator iter = active_instances_.find(_conn);
  if (iter == active_instances_.end()) {
    LOG(ERROR) << "Hmm.. Got InstaceStateCheckpoint Message from an unknown connection";
    return;
  }

  sp_int32 task_id = iter->second;
  TaskIdInstanceDataMap::iterator it = instance_info_.find(task_id);
  if (it == instance_info_.end()) {
    LOG(ERROR) << "Hmm.. Got InstaceStateCheckpoint Message from unknown task_id "
               << task_id;
    return;
  }

  // send the checkpoint message to all downstream task ids
  stmgr_->HandleStoreInstanceStateCheckpoint(_message->state(), *(it->second->instance_));
}

void InstanceServer::HandleRestoreInstanceStateResponse(Connection* _conn,
                          pool_unique_ptr<proto::ckptmgr::RestoreInstanceStateResponse> _message) {
  ConnectionTaskIdMap::iterator iter = active_instances_.find(_conn);
  if (iter == active_instances_.end()) {
    LOG(ERROR) << "Hmm.. Got RestoreInstanceStateResponse Message from an unknown connection";
    return;
  }

  sp_int32 task_id = iter->second;
  TaskIdInstanceDataMap::iterator it = instance_info_.find(task_id);
  if (it == instance_info_.end()) {
    LOG(ERROR) << "Hmm.. Got RestoreInstanceStateResponse Message from unknown task_id "
               << task_id;
    return;
  }

  // send the checkpoint message to all downstream task ids
  stmgr_->HandleRestoreInstanceStateResponse(task_id, _message->status(),
                                             _message->checkpoint_id());
}

void InstanceServer::HandleCheckpointMarker(sp_int32 _src_task_id, sp_int32 _destination_task_id,
                                         const sp_string& _checkpoint_id) {
  // So received a checkpoint marker from an upstream task
  stateful_gateway_->HandleUpstreamMarker(_src_task_id, _destination_task_id, _checkpoint_id);
}

bool InstanceServer::SendRestoreInstanceStateRequest(sp_int32 _task_id,
            const proto::ckptmgr::InstanceStateCheckpoint& _state) {
  LOG(INFO) << "Sending RestoreInstanceState request to task " << _task_id;
  if (instance_info_.find(_task_id) == instance_info_.end()) {
    LOG(WARNING) << "Cannot send RestoreInstanceState Request to task "
                 << _task_id << " because it is not connected to us";
    return false;
  }
  Connection* conn = instance_info_[_task_id]->conn_;
  if (conn) {
    auto message = make_unique<proto::ckptmgr::RestoreInstanceStateRequest>();
    message->mutable_state()->CopyFrom(_state);
    SendMessage(conn, *message);
    return true;
  } else {
    LOG(WARNING) << "Cannot send RestoreInstanceState Request to task "
                 << _task_id << " because it is not connected to us";
    return false;
  }
}

void InstanceServer::SendStartInstanceStatefulProcessing(const std::string& _ckpt_id) {
  for (auto kv : instance_info_) {
    Connection* conn = kv.second->conn_;
    if (conn) {
      auto message = make_unique<proto::ckptmgr::StartInstanceStatefulProcessing>();
      message->set_checkpoint_id(_ckpt_id);
      SendMessage(conn, *message);
    } else {
      LOG(WARNING) << "Cannot send StartInstanceStatefulProcessing to task "
                   << kv.first << " because it is not connected to us";
    }
  }
}

void InstanceServer::ClearCache() {
  stateful_gateway_->Clear();
}
}  // namespace stmgr
}  // namespace heron
