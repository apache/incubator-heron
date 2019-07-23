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

#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_CHECKPOINT_GATEWAY_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_CHECKPOINT_GATEWAY_H_

#include <unordered_map>
#include <unordered_set>
#include <deque>
#include <tuple>
#include <utility>
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

namespace heron {
namespace common {
class MetricsMgrSt;
class AssignableMetric;
}  // namespace common
}  // namespace heron

namespace heron {
namespace stmgr {

class NeighbourCalculator;

using std::unique_ptr;
using std::shared_ptr;
using proto::ckptmgr::InitiateStatefulCheckpoint;

// The CheckpointGateway class defines the buffer inside the stmgr
// that exists to buffer tuples to all the local instances until
// all upstream checkpoint markers are received. The gateway
// buffers the tuples uptil a certain threshold after which the
// markers are discarded
class CheckpointGateway {
 public:
  explicit CheckpointGateway(sp_uint64 _drain_threshold,
        shared_ptr<NeighbourCalculator> _neighbour_calculator,
        shared_ptr<common::MetricsMgrSt> const& _metrics_manager_client,
        std::function<void(sp_int32, proto::system::HeronTupleSet2*)> tupleset_drainer,
        std::function<void(proto::stmgr::TupleStreamMessage*)> tuplestream_drainer,
        std::function<void(sp_int32, pool_unique_ptr<InitiateStatefulCheckpoint>)> ckpt_drainer);
  virtual ~CheckpointGateway();
  void SendToInstance(sp_int32 _task_id, proto::system::HeronTupleSet2* _message);
  void SendToInstance(pool_unique_ptr<proto::stmgr::TupleStreamMessage> _message);
  void HandleUpstreamMarker(sp_int32 _src_task_id, sp_int32 _destination_task_id,
                            const sp_string& _checkpoint_id);

  // Clears all tuples
  void Clear();

 private:
  typedef std::tuple<proto::system::HeronTupleSet2*,
                     proto::stmgr::TupleStreamMessage*,
                     pool_unique_ptr<proto::ckptmgr::InitiateStatefulCheckpoint>>
          Tuple;

  // This helper class defines the current state of affairs
  // for a particular local instance(this_task_id_)
  class CheckpointInfo {
   public:
    explicit CheckpointInfo(sp_int32 _this_task_id,
                            const std::unordered_set<sp_int32>& _all_upstream_dependencies);
    ~CheckpointInfo();
    proto::system::HeronTupleSet2*  SendToInstance(proto::system::HeronTupleSet2* _tuple,
                                                   sp_uint64 _size);
    proto::stmgr::TupleStreamMessage* SendToInstance(proto::stmgr::TupleStreamMessage* _tuple,
                                                      sp_uint64 _size);
    std::deque<Tuple> HandleUpstreamMarker(sp_int32 _src_task_id,
                                             const sp_string& _checkpoint_id, sp_uint64* _size);
    std::deque<Tuple> ForceDrain();
    void Clear();
   private:
    void add(Tuple& _tuple, sp_uint64 _size);
    void add_front(Tuple& _tuple, sp_uint64 _size);
    sp_string checkpoint_id_;
    std::unordered_set<sp_int32> all_upstream_dependencies_;
    std::unordered_set<sp_int32> pending_upstream_dependencies_;
    std::deque<Tuple> pending_tuples_;
    sp_uint64 current_size_;
    sp_int32 this_task_id_;
  };
  void ForceDrain();
  void DrainTuple(sp_int32 _dest, Tuple& _tuple);
  CheckpointGateway::CheckpointInfo& get_info(sp_int32 _task_id);

  // The maximum buffering that we can do before we discard the marker
  sp_uint64 drain_threshold_;
  sp_uint64 current_size_;
  shared_ptr<NeighbourCalculator> neighbour_calculator_;
  shared_ptr<common::MetricsMgrSt> metrics_manager_client_;
  std::shared_ptr<common::AssignableMetric> size_metric_;
  std::unordered_map<sp_int32, unique_ptr<CheckpointInfo>> pending_tuples_;
  std::function<void(sp_int32, proto::system::HeronTupleSet2*)> tupleset_drainer_;
  std::function<void(proto::stmgr::TupleStreamMessage*)> tuplestream_drainer_;
  std::function<void(sp_int32, pool_unique_ptr<InitiateStatefulCheckpoint>)> ckpt_drainer_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_CHECKPOINT_GATEWAY_H_
