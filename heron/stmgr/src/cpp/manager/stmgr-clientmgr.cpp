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

#include "manager/stmgr-clientmgr.h"
#include <iostream>
#include <map>
#include <unordered_set>
#include "manager/stmgr.h"
#include "manager/stmgr-client.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "config/heron-internals-config-reader.h"
#include "metrics/metrics.h"

namespace heron {
namespace stmgr {

using std::make_shared;

// New connections made with other stream managers.
const sp_string METRIC_STMGR_NEW_CONNECTIONS = "__stmgr_new_connections";

StMgrClientMgr::StMgrClientMgr(shared_ptr<EventLoop> eventLoop, const sp_string& _topology_name,
                             const sp_string& _topology_id, const sp_string& _stmgr_id,
                             StMgr* _stream_manager,
                             shared_ptr<heron::common::MetricsMgrSt> const& _metrics_manager_client,
                             sp_int64 _high_watermark, sp_int64 _low_watermark,
                             bool _droptuples_upon_backpressure)
    : topology_name_(_topology_name),
      topology_id_(_topology_id),
      stmgr_id_(_stmgr_id),
      eventLoop_(eventLoop),
      stream_manager_(_stream_manager),
      metrics_manager_client_(_metrics_manager_client),
      high_watermark_(_high_watermark),
      low_watermark_(_low_watermark),
      droptuples_upon_backpressure_(_droptuples_upon_backpressure) {
  stmgr_clientmgr_metrics_ = make_shared<heron::common::MultiCountMetric>();
  metrics_manager_client_->register_metric("__clientmgr", stmgr_clientmgr_metrics_);
}

StMgrClientMgr::~StMgrClientMgr() {
  // This should not be called
  metrics_manager_client_->unregister_metric("__clientmgr");
  clients_.clear();
}

void StMgrClientMgr::StartConnections(proto::system::PhysicalPlan const& _pplan) {
  // TODO(vikasr) : Currently we establish connections with all streammanagers
  // In the next iteration we might want to make it better
  std::unordered_set<sp_string> all_stmgrs;
  for (sp_int32 i = 0; i < _pplan.stmgrs_size(); ++i) {
    const proto::system::StMgr& s = _pplan.stmgrs(i);
    if (s.id() == stmgr_id_) {
      continue;  // dont want to connect to ourselves
    }
    all_stmgrs.insert(s.id());
    if (clients_.find(s.id()) != clients_.end()) {
      // We already have a connection for this stmgr.
      // Just make sure we have it for the same host/port
      const NetworkOptions& o = clients_[s.id()]->get_clientoptions();
      if (o.get_host() != s.host_name() || o.get_port() != s.data_port()) {
        LOG(INFO) << "Stmgr " << s.id() << " changed from " << o.get_host() << ":" << o.get_port()
                  << " to " << s.host_name() << ":" << s.data_port();
        // This stmgr has actually moved to a different host/port
        clients_[s.id()]->Quit();  // this will delete itself.
        clients_[s.id()] = CreateClient(s.id(), s.host_name(), s.data_port());
      } else {
        // This stmgr has remained the same. Don't do anything
      }
    } else {
      // We don't have any connection to this stmgr.
      LOG(INFO) << "Stmgr " << s.id() << " came on " << s.host_name() << ":" << s.data_port();
      clients_[s.id()] = CreateClient(s.id(), s.host_name(), s.data_port());
    }
  }

  // We need to remove any unused ports
  std::unordered_set<sp_string> to_remove;
  for (auto iter = clients_.begin(); iter != clients_.end(); ++iter) {
    if (all_stmgrs.find(iter->first) == all_stmgrs.end()) {
      // This stmgr is no longer there in the physical map
      to_remove.insert(iter->first);
    }
  }

  // Now go over to_remove to remove all the unused stmgrs
  for (auto iter = to_remove.begin(); iter != to_remove.end(); ++iter) {
    LOG(INFO) << "Stmgr " << *iter << " no longer required";
    clients_[*iter]->Quit();  // This will delete itself.
    clients_.erase(*iter);
  }
}

bool StMgrClientMgr::DidAnnounceBackPressure() {
  return stream_manager_->DidAnnounceBackPressure();
}

StMgrClient* StMgrClientMgr::CreateClient(const sp_string& _other_stmgr_id,
                                          const sp_string& _hostname, sp_int32 _port) {
  stmgr_clientmgr_metrics_->scope(METRIC_STMGR_NEW_CONNECTIONS)->incr();
  NetworkOptions options;
  options.set_host(_hostname);
  options.set_port(_port);
  options.set_max_packet_size(config::HeronInternalsConfigReader::Instance()
                                  ->GetHeronStreammgrNetworkOptionsMaximumPacketMb() * 1_MB);
  options.set_high_watermark(high_watermark_);
  options.set_low_watermark(low_watermark_);
  options.set_socket_family(PF_INET);
  StMgrClient* client = new StMgrClient(eventLoop_, options, topology_name_, topology_id_,
                                        stmgr_id_, _other_stmgr_id, this, metrics_manager_client_,
                                        droptuples_upon_backpressure_);
  client->Start();
  return client;
}

bool StMgrClientMgr::SendTupleStreamMessage(sp_int32 _task_id, const sp_string& _stmgr_id,
                                            const proto::system::HeronTupleSet2& _msg) {
  auto iter = clients_.find(_stmgr_id);
  CHECK(iter != clients_.end());

  // Acquire the message
  proto::stmgr::TupleStreamMessage* out = nullptr;
  out = __global_protobuf_pool_acquire__(out);
  out->set_task_id(_task_id);
  out->set_src_task_id(_msg.src_task_id());
  sp_int32 length = 0;
  if (_msg.has_data()) {
    length = _msg.data().tuples_size();
  }
  out->set_num_tuples(length);
  _msg.SerializePartialToString(out->mutable_set());

  bool retval = clients_[_stmgr_id]->SendTupleStreamMessage(*out);

  // Release the message
  __global_protobuf_pool_release__(out);

  return retval;
}

void StMgrClientMgr::SendDownstreamStatefulCheckpoint(const sp_string& _stmgr_id,
                           proto::ckptmgr::DownstreamStatefulCheckpoint* _message) {
  auto iter = clients_.find(_stmgr_id);
  CHECK(iter != clients_.end());
  iter->second->SendDownstreamStatefulCheckpoint(_message);
}

void StMgrClientMgr::StartBackPressureOnServer(const sp_string& _other_stmgr_id) {
  stream_manager_->StartBackPressureOnServer(_other_stmgr_id);
}

void StMgrClientMgr::StopBackPressureOnServer(const sp_string& _other_stmgr_id) {
  // Call the StMgrServers removeBackPressure method
  stream_manager_->StopBackPressureOnServer(_other_stmgr_id);
}

void StMgrClientMgr::SendStartBackPressureToOtherStMgrs() {
  for (auto iter = clients_.begin(); iter != clients_.end(); ++iter) {
    iter->second->SendStartBackPressureMessage();
  }
}

void StMgrClientMgr::SendStopBackPressureToOtherStMgrs() {
  for (auto iter = clients_.begin(); iter != clients_.end(); ++iter) {
    iter->second->SendStopBackPressureMessage();
  }
}

void StMgrClientMgr::HandleDeadStMgrConnection(const sp_string& _dead_stmgr) {
  stream_manager_->HandleDeadStMgrConnection(_dead_stmgr);
}

void StMgrClientMgr::HandleStMgrClientRegistered() {
  if (AllStMgrClientsRegistered()) {
    stream_manager_->HandleAllStMgrClientsRegistered();
  }
}

void StMgrClientMgr::CloseConnectionsAndClear() {
  for (auto kv : clients_) {
    kv.second->Quit();  // It will delete itself
  }
  clients_.clear();
}

bool StMgrClientMgr::AllStMgrClientsRegistered() {
  for (auto kv : clients_) {
    if (!kv.second->IsConnected()) {
      return false;
    }
    if (!kv.second->IsRegistered()) {
      return false;
    }
  }
  return true;
}
}  // namespace stmgr
}  // namespace heron
