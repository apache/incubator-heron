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

#include "statemgr/heron-zkstatemgr.h"
#include <iostream>
#include <string>
#include <vector>
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"
#include "zookeeper/zkclient.h"

namespace heron {
namespace common {

HeronZKStateMgr::HeronZKStateMgr(const std::string& zkhostport, const std::string& topleveldir,
                                 EventLoop* eventLoop, bool exitOnSessionExpiry)
    : HeronStateMgr(topleveldir),
      zkhostport_(zkhostport),
      zkclient_(NULL),
      zkclient_factory_(new DefaultZKClientFactory()),
      eventLoop_(eventLoop),
      tmaster_location_watcher_info_(NULL),
      exitOnSessionExpiry_(exitOnSessionExpiry) {
  Init();
}

HeronZKStateMgr::HeronZKStateMgr(const std::string& zkhostport, const std::string& topleveldir,
                                 EventLoop* eventLoop, ZKClientFactory* zkclient_factory,
                                 bool exitOnSessionExpiry)
    : HeronStateMgr(topleveldir),
      zkhostport_(zkhostport),
      zkclient_(NULL),
      zkclient_factory_(zkclient_factory),
      eventLoop_(eventLoop),
      tmaster_location_watcher_info_(NULL),
      exitOnSessionExpiry_(exitOnSessionExpiry) {
  Init();
}

void HeronZKStateMgr::Init() {
  if (exitOnSessionExpiry_) {
    watch_event_cb_ = VCallback<ZKClient::ZkWatchEvent>();
  } else {
    watch_event_cb_ = [this](ZKClient::ZkWatchEvent event) {
      this->GlobalWatchEventHandler(event);
    };
  }

  // If watch_event_cb is empty, zkClient exits on session expired event
  zkclient_ = zkclient_factory_->create(zkhostport_, eventLoop_, watch_event_cb_);
}

HeronZKStateMgr::~HeronZKStateMgr() {
  delete zkclient_;
  delete zkclient_factory_;
  delete tmaster_location_watcher_info_;
}

void HeronZKStateMgr::InitTree() {
  // Needs to be implemented
  CHECK(false);
}

void HeronZKStateMgr::SetTMasterLocationWatch(const std::string& topology_name,
                                              VCallback<> watcher) {
  CHECK(watcher);
  CHECK(!topology_name.empty());

  tmaster_location_watcher_info_ = new TMasterLocationWatchInfo(std::move(watcher), topology_name);
  SetTMasterLocationWatchInternal();
}

void HeronZKStateMgr::SetMetricsCacheLocationWatch(const std::string& topology_name,
                                              VCallback<> watcher) {
  CHECK(watcher);
  CHECK(!topology_name.empty());

  metricscache_location_watcher_info_ = new TMasterLocationWatchInfo(
                              std::move(watcher), topology_name);
  SetMetricsCacheLocationWatchInternal();
}

void HeronZKStateMgr::SetTMasterLocation(const proto::tmaster::TMasterLocation& _location,
                                         VCallback<proto::system::StatusCode> cb) {
  // Just try to create an ephimeral node
  std::string path = GetTMasterLocationPath(_location.topology_name());
  std::string value;
  _location.SerializeToString(&value);

  auto wCb = [cb, this](sp_int32 rc) { this->SetTMasterLocationDone(std::move(cb), rc); };
  zkclient_->CreateNode(path, value, true, std::move(wCb));
}

void HeronZKStateMgr::SetMetricsCacheLocation(const proto::tmaster::MetricsCacheLocation& _location,
                                         VCallback<proto::system::StatusCode> cb) {
  // Just try to create an ephimeral node
  std::string path = GetMetricsCacheLocationPath(_location.topology_name());
  std::string value;
  _location.SerializeToString(&value);

  auto wCb = [cb, this](sp_int32 rc) { this->SetMetricsCacheLocationDone(std::move(cb), rc); };
  zkclient_->CreateNode(path, value, true, std::move(wCb));
}

void HeronZKStateMgr::GetTMasterLocation(const std::string& _topology_name,
                                         proto::tmaster::TMasterLocation* _return,
                                         VCallback<proto::system::StatusCode> cb) {
  std::string path = GetTMasterLocationPath(_topology_name);
  std::string* contents = new std::string();

  auto wCb = [contents, _return, cb, this](sp_int32 rc) {
    this->GetTMasterLocationDone(contents, _return, std::move(cb), rc);
  };

  zkclient_->Get(path, contents, std::move(wCb));
}

void HeronZKStateMgr::GetMetricsCacheLocation(const std::string& _topology_name,
                                         proto::tmaster::MetricsCacheLocation* _return,
                                         VCallback<proto::system::StatusCode> cb) {
  std::string path = GetMetricsCacheLocationPath(_topology_name);
  std::string* contents = new std::string();

  auto wCb = [contents, _return, cb, this](sp_int32 rc) {
    this->GetMetricsCacheLocationDone(contents, _return, std::move(cb), rc);
  };

  zkclient_->Get(path, contents, std::move(wCb));
}

void HeronZKStateMgr::CreateTopology(const proto::api::Topology& _topology,
                                     VCallback<proto::system::StatusCode> cb) {
  std::string path = GetTopologyPath(_topology.name());
  std::string value;
  _topology.SerializeToString(&value);
  auto wCb = [cb, this](sp_int32 rc) { this->CreateTopologyDone(std::move(cb), rc); };
  zkclient_->CreateNode(path, value, false, std::move(wCb));
}

void HeronZKStateMgr::DeleteTopology(const std::string& _topology_name,
                                     VCallback<proto::system::StatusCode> cb) {
  std::string path = GetTopologyPath(_topology_name);
  auto wCb = [cb, this](sp_int32 rc) { this->DeleteTopologyDone(std::move(cb), rc); };
  zkclient_->DeleteNode(path, std::move(wCb));
}

void HeronZKStateMgr::SetTopology(const proto::api::Topology& _topology,
                                  VCallback<proto::system::StatusCode> cb) {
  std::string path = GetTopologyPath(_topology.name());
  std::string value;
  _topology.SerializeToString(&value);
  auto wCb = [cb, this](sp_int32 rc) { this->SetTopologyDone(std::move(cb), rc); };
  zkclient_->Set(path, value, std::move(wCb));
}

void HeronZKStateMgr::GetTopology(const std::string& _topology_name, proto::api::Topology* _return,
                                  VCallback<proto::system::StatusCode> cb) {
  std::string path = GetTopologyPath(_topology_name);
  std::string* contents = new std::string();

  auto wCb = [contents, _return, cb, this](sp_int32 rc) {
    this->GetTopologyDone(contents, _return, std::move(cb), rc);
  };

  zkclient_->Get(path, contents, std::move(wCb));
}

void HeronZKStateMgr::CreatePhysicalPlan(const proto::system::PhysicalPlan& _pplan,
                                         VCallback<proto::system::StatusCode> cb) {
  std::string path = GetPhysicalPlanPath(_pplan.topology().name());
  std::string contents;
  _pplan.SerializeToString(&contents);

  auto wCb = [cb, this](sp_int32 rc) { this->CreatePhysicalPlanDone(std::move(cb), rc); };
  zkclient_->CreateNode(path, contents, false, std::move(wCb));
}

void HeronZKStateMgr::DeletePhysicalPlan(const std::string& _topology_name,
                                         VCallback<proto::system::StatusCode> cb) {
  std::string path = GetPhysicalPlanPath(_topology_name);
  auto wCb = [cb, this](sp_int32 rc) { this->DeletePhysicalPlanDone(std::move(cb), rc); };
  zkclient_->DeleteNode(path, std::move(wCb));
}

void HeronZKStateMgr::SetPhysicalPlan(const proto::system::PhysicalPlan& _pplan,
                                      VCallback<proto::system::StatusCode> cb) {
  std::string path = GetPhysicalPlanPath(_pplan.topology().name());
  std::string contents;
  _pplan.SerializeToString(&contents);

  auto wCb = [cb, this](sp_int32 rc) { this->SetPhysicalPlanDone(std::move(cb), rc); };

  zkclient_->Set(path, contents, std::move(wCb));
}

void HeronZKStateMgr::GetPhysicalPlan(const std::string& _topology_name,
                                      proto::system::PhysicalPlan* _return,
                                      VCallback<proto::system::StatusCode> cb) {
  std::string path = GetPhysicalPlanPath(_topology_name);
  std::string* contents = new std::string();
  auto wCb = [contents, _return, cb, this](sp_int32 rc) {
    this->GetPhysicalPlanDone(contents, _return, std::move(cb), rc);
  };

  zkclient_->Get(path, contents, std::move(wCb));
}

void HeronZKStateMgr::CreateExecutionState(const proto::system::ExecutionState& _state,
                                           VCallback<proto::system::StatusCode> cb) {
  std::string path = GetExecutionStatePath(_state.topology_name());
  std::string contents;
  _state.SerializeToString(&contents);
  auto wCb = [cb, this](sp_int32 rc) { this->CreateExecutionStateDone(std::move(cb), rc); };

  zkclient_->CreateNode(path, contents, false, std::move(wCb));
}

void HeronZKStateMgr::DeleteExecutionState(const std::string& _topology_name,
                                           VCallback<proto::system::StatusCode> cb) {
  std::string path = GetExecutionStatePath(_topology_name);
  auto wCb = [cb, this](sp_int32 rc) { this->DeleteExecutionStateDone(std::move(cb), rc); };

  zkclient_->DeleteNode(path, std::move(wCb));
}

void HeronZKStateMgr::SetExecutionState(const proto::system::ExecutionState& _state,
                                        VCallback<proto::system::StatusCode> cb) {
  std::string path = GetExecutionStatePath(_state.topology_name());
  std::string contents;
  _state.SerializeToString(&contents);
  auto wCb = [cb, this](sp_int32 rc) { this->SetExecutionStateDone(std::move(cb), rc); };

  zkclient_->Set(path, contents, std::move(wCb));
}

void HeronZKStateMgr::GetExecutionState(const std::string& _topology_name,
                                        proto::system::ExecutionState* _return,
                                        VCallback<proto::system::StatusCode> cb) {
  std::string path = GetExecutionStatePath(_topology_name);
  std::string* contents = new std::string();
  auto wCb = [contents, _return, cb, this](sp_int32 rc) {
    this->GetExecutionStateDone(contents, _return, std::move(cb), rc);
  };

  zkclient_->Get(path, contents, std::move(wCb));
}

void HeronZKStateMgr::ListTopologies(std::vector<sp_string>* _return,
                                     VCallback<proto::system::StatusCode> cb) {
  sp_string path = GetTopologyDir();
  auto wCb = [cb, this](sp_int32 rc) { this->ListTopologiesDone(std::move(cb), rc); };

  zkclient_->GetChildren(path, _return, wCb);
}

void HeronZKStateMgr::ListExecutionStateTopologies(std::vector<sp_string>* _return,
                                                   VCallback<proto::system::StatusCode> cb) {
  sp_string path = GetExecutionStateDir();
  auto wCb = [cb, this](sp_int32 rc) { this->ListExecutionStateTopologiesDone(std::move(cb), rc); };

  zkclient_->GetChildren(path, _return, std::move(wCb));
}

void HeronZKStateMgr::GlobalWatchEventHandler(const ZKClient::ZkWatchEvent event) {
  LOG(INFO) << "Received an event, Type: " << ZKClient::type2String(event.type)
            << ", State: " << ZKClient::state2String(event.state);

  if (event.type == ZOO_SESSION_EVENT && event.state == ZOO_EXPIRED_SESSION_STATE) {
    // TODO(kramasamy): The session expired event is only triggered after the client
    // is able to connect back to the zk server after a connection loss. But the
    // duration of the connection loss is indeterminate, so it is pointless to
    // wait for the entire duration. A better approach here is to timeout after
    // client is in connecting state for a duration greater than session timeout.
    LOG(INFO) << "Deleting current zk client... ";
    // This could be a blocking call since it flushes out all outstanding
    // requests. Hence adding logs before and after to track time consumed.
    // NOTE: Since this class is meant to be operate in single threaded mode,
    // this is a safe operation.
    delete zkclient_;
    LOG(INFO) << "Deleted current zk client, creating a new one...";
    zkclient_ = zkclient_factory_->create(zkhostport_, eventLoop_, watch_event_cb_);
    LOG(INFO) << "New zk client created";
    // set tmaster watch and notify the client watcher
    // NOTE: It isn't enough to just set the watch here, since we could
    // have lost a tmaster node change when the session expired. This is needed
    // since the current zkclient design notifies only the "Connected_State" events to
    // the individual node watchers. Session expired events need explicit notification.
    if (IsTmasterWatchDefined()) {
      TMasterLocationWatch();
    }
  } else {
    LOG(WARNING) << "Events other than session expired event are not"
                 << "expected, at least for now" << std::endl;
  }
}

void HeronZKStateMgr::SetTMasterLocationDone(VCallback<proto::system::StatusCode> cb,
                                             sp_int32 _rc) {
  proto::system::StatusCode code = proto::system::OK;
  if (_rc == ZNODEEXISTS) {
    LOG(ERROR) << "Setting TMaster Location failed because another zmaster exists" << std::endl;
    code = proto::system::TMASTERLOCATION_ALREADY_EXISTS;
  } else if (_rc != ZOK) {
    LOG(ERROR) << "Setting TMaster Location failed with error " << _rc << std::endl;
    code = proto::system::STATE_WRITE_ERROR;
  }

  cb(code);
}

void HeronZKStateMgr::SetMetricsCacheLocationDone(VCallback<proto::system::StatusCode> cb,
                                             sp_int32 _rc) {
  proto::system::StatusCode code = proto::system::OK;
  if (_rc == ZNODEEXISTS) {
    LOG(ERROR) << "Setting MetricsCache Location failed because another zmaster exists"
               << std::endl;
    code = proto::system::METRICSCACHELOCATION_ALREADY_EXISTS;
  } else if (_rc != ZOK) {
    LOG(ERROR) << "Setting MetricsCache Location failed with error " << _rc << std::endl;
    code = proto::system::STATE_WRITE_ERROR;
  }

  cb(code);
}

void HeronZKStateMgr::GetTMasterLocationDone(std::string* _contents,
                                             proto::tmaster::TMasterLocation* _return,
                                             VCallback<proto::system::StatusCode> cb,
                                             sp_int32 _rc) {
  proto::system::StatusCode code = proto::system::OK;
  if (_rc == ZOK) {
    if (!_return->ParseFromString(*_contents)) {
      LOG(ERROR) << "Error parsing tmaster location" << std::endl;
      code = proto::system::STATE_CORRUPTED;
    }
  } else if (_rc == ZNONODE) {
    LOG(ERROR) << "Error getting tmaster location because the tmaster does not exist" << std::endl;
    code = proto::system::PATH_DOES_NOT_EXIST;
  } else {
    LOG(ERROR) << "Getting TMaster Location failed with error " << _rc << std::endl;
    code = proto::system::STATE_READ_ERROR;
  }
  delete _contents;
  cb(code);
}

void HeronZKStateMgr::GetMetricsCacheLocationDone(std::string* _contents,
                                             proto::tmaster::MetricsCacheLocation* _return,
                                             VCallback<proto::system::StatusCode> cb,
                                             sp_int32 _rc) {
  proto::system::StatusCode code = proto::system::OK;
  if (_rc == ZOK) {
    if (!_return->ParseFromString(*_contents)) {
      LOG(ERROR) << "Error parsing metricscache location" << std::endl;
      code = proto::system::STATE_CORRUPTED;
    }
  } else if (_rc == ZNONODE) {
    LOG(ERROR) << "Error getting metricscache location because the tmaster does not exist"
               << std::endl;
    code = proto::system::PATH_DOES_NOT_EXIST;
  } else {
    LOG(ERROR) << "Getting MetricsCache Location failed with error " << _rc << std::endl;
    code = proto::system::STATE_READ_ERROR;
  }
  delete _contents;
  cb(code);
}

void HeronZKStateMgr::CreateTopologyDone(VCallback<proto::system::StatusCode> cb, sp_int32 _rc) {
  proto::system::StatusCode code = proto::system::OK;
  if (_rc == ZNONODE) {
    LOG(ERROR) << "Setting Topology failed because zk is not setup properly" << std::endl;
    code = proto::system::PATH_DOES_NOT_EXIST;
  } else if (_rc != ZOK) {
    LOG(ERROR) << "Creating Topology failed with error " << _rc << std::endl;
    code = proto::system::STATE_WRITE_ERROR;
  }
  cb(code);
}

void HeronZKStateMgr::DeleteTopologyDone(VCallback<proto::system::StatusCode> cb, sp_int32 _rc) {
  proto::system::StatusCode code = proto::system::OK;
  if (_rc == ZNONODE) {
    LOG(ERROR) << "Deleting Topology failed because there was no such node" << std::endl;
    code = proto::system::PATH_DOES_NOT_EXIST;
  } else if (_rc != ZOK) {
    LOG(ERROR) << "Setting Topology failed with error " << _rc << std::endl;
    code = proto::system::STATE_WRITE_ERROR;
  }
  cb(code);
}

void HeronZKStateMgr::SetTopologyDone(VCallback<proto::system::StatusCode> cb, sp_int32 _rc) {
  proto::system::StatusCode code = proto::system::OK;
  if (_rc == ZNONODE) {
    LOG(ERROR) << "Setting Topology failed because topoloogy does not exist" << std::endl;
    code = proto::system::PATH_DOES_NOT_EXIST;
  } else if (_rc != ZOK) {
    LOG(ERROR) << "Setting Topology failed with error " << _rc << std::endl;
    code = proto::system::STATE_WRITE_ERROR;
  }
  cb(code);
}

void HeronZKStateMgr::GetTopologyDone(std::string* _contents, proto::api::Topology* _return,
                                      VCallback<proto::system::StatusCode> cb, sp_int32 _rc) {
  proto::system::StatusCode code = proto::system::OK;
  if (_rc == ZOK) {
    if (!_return->ParseFromString(*_contents)) {
      LOG(ERROR) << "topology parsing failed; zk corruption?" << std::endl;
      code = proto::system::STATE_CORRUPTED;
    }
  } else if (_rc == ZNONODE) {
    LOG(ERROR) << "Error getting topology because the topology does not exist" << std::endl;
    code = proto::system::PATH_DOES_NOT_EXIST;
  } else {
    LOG(ERROR) << "Getting Topology failed with error " << _rc << std::endl;
    code = proto::system::STATE_READ_ERROR;
  }
  delete _contents;
  cb(code);
}

void HeronZKStateMgr::CreatePhysicalPlanDone(VCallback<proto::system::StatusCode> cb,
                                             sp_int32 _rc) {
  proto::system::StatusCode code = proto::system::OK;
  if (_rc == ZNONODE) {
    LOG(ERROR) << "Creating Physical Plan failed because zk was not setup properly" << std::endl;
    code = proto::system::PATH_DOES_NOT_EXIST;
  } else if (_rc != ZOK) {
    LOG(ERROR) << "Setting Physical Plan failed with error " << _rc << std::endl;
    code = proto::system::STATE_WRITE_ERROR;
  }
  cb(code);
}

void HeronZKStateMgr::DeletePhysicalPlanDone(VCallback<proto::system::StatusCode> cb,
                                             sp_int32 _rc) {
  proto::system::StatusCode code = proto::system::OK;
  if (_rc == ZNONODE) {
    LOG(ERROR) << "Deleting Physical Plan failed because there was no such node" << std::endl;
    code = proto::system::PATH_DOES_NOT_EXIST;
  } else if (_rc != ZOK) {
    LOG(ERROR) << "Deleting Physical Plan failed with error " << _rc << std::endl;
    code = proto::system::STATE_WRITE_ERROR;
  }
  cb(code);
}

void HeronZKStateMgr::SetPhysicalPlanDone(VCallback<proto::system::StatusCode> cb, sp_int32 _rc) {
  proto::system::StatusCode code = proto::system::OK;
  if (_rc == ZNONODE) {
    LOG(ERROR) << "Setting Physical Plan failed because there was no such node" << std::endl;
    code = proto::system::PATH_DOES_NOT_EXIST;
  } else if (_rc != ZOK) {
    LOG(ERROR) << "Setting Assignment failed with error " << _rc << std::endl;
    code = proto::system::STATE_WRITE_ERROR;
  }
  cb(code);
}

void HeronZKStateMgr::GetPhysicalPlanDone(std::string* _contents,
                                          proto::system::PhysicalPlan* _return,
                                          VCallback<proto::system::StatusCode> cb, sp_int32 _rc) {
  proto::system::StatusCode code = proto::system::OK;
  if (_rc == ZOK) {
    if (!_return->ParseFromString(*_contents)) {
      code = proto::system::STATE_CORRUPTED;
    }
  } else if (_rc == ZNONODE) {
    code = proto::system::PATH_DOES_NOT_EXIST;
  } else {
    LOG(ERROR) << "Getting PhysicalPlan failed with error " << _rc << std::endl;
    code = proto::system::STATE_READ_ERROR;
  }
  delete _contents;
  cb(code);
}

void HeronZKStateMgr::CreateExecutionStateDone(VCallback<proto::system::StatusCode> cb,
                                               sp_int32 _rc) {
  proto::system::StatusCode code = proto::system::OK;
  if (_rc == ZNONODE) {
    LOG(ERROR) << "Creating ExecutionState failed because zookeeper was not setup properly"
               << std::endl;
    code = proto::system::PATH_DOES_NOT_EXIST;
  } else if (_rc != ZOK) {
    LOG(ERROR) << "Creating ExecutionState failed with error " << _rc << std::endl;
    code = proto::system::STATE_WRITE_ERROR;
  }
  cb(code);
}

void HeronZKStateMgr::DeleteExecutionStateDone(VCallback<proto::system::StatusCode> cb,
                                               sp_int32 _rc) {
  proto::system::StatusCode code = proto::system::OK;
  if (_rc == ZNONODE) {
    LOG(ERROR) << "Deleting ExecutionState failed because the node does not exists" << std::endl;
    code = proto::system::PATH_DOES_NOT_EXIST;
  } else if (_rc != ZOK) {
    LOG(ERROR) << "Deleting ExecutionState failed with error " << _rc << std::endl;
    code = proto::system::STATE_WRITE_ERROR;
  } else {
    LOG(ERROR) << "Deleted Exectution state" << std::endl;
  }
  cb(code);
}

void HeronZKStateMgr::SetExecutionStateDone(VCallback<proto::system::StatusCode> cb, sp_int32 _rc) {
  proto::system::StatusCode code = proto::system::OK;
  if (_rc == ZNONODE) {
    LOG(ERROR) << "Setting Execution State failed because there was no such node" << std::endl;
    code = proto::system::PATH_DOES_NOT_EXIST;
  } else if (_rc != ZOK) {
    LOG(ERROR) << "Setting Execution state failed with error " << _rc << std::endl;
    code = proto::system::STATE_WRITE_ERROR;
  }
  cb(code);
}

void HeronZKStateMgr::GetExecutionStateDone(std::string* _contents,
                                            proto::system::ExecutionState* _return,
                                            VCallback<proto::system::StatusCode> cb, sp_int32 _rc) {
  proto::system::StatusCode code = proto::system::OK;
  if (_rc == ZOK) {
    if (!_return->ParseFromString(*_contents)) {
      code = proto::system::STATE_CORRUPTED;
    }
  } else if (_rc == ZNONODE) {
    code = proto::system::PATH_DOES_NOT_EXIST;
  } else {
    LOG(ERROR) << "Getting ExecutionState failed with error " << _rc << std::endl;
    code = proto::system::STATE_READ_ERROR;
  }
  delete _contents;
  cb(code);
}

void HeronZKStateMgr::ListTopologiesDone(VCallback<proto::system::StatusCode> cb, sp_int32 _rc) {
  proto::system::StatusCode code = proto::system::OK;
  if (_rc != ZOK) {
    code = proto::system::NOTOK;
  }
  cb(code);
}

void HeronZKStateMgr::ListExecutionStateTopologiesDone(VCallback<proto::system::StatusCode> cb,
                                                       sp_int32 _rc) {
  proto::system::StatusCode code = proto::system::OK;
  if (_rc != ZOK) {
    code = proto::system::NOTOK;
  }
  cb(code);
}

bool HeronZKStateMgr::IsTmasterWatchDefined() {
  return (tmaster_location_watcher_info_ != NULL && tmaster_location_watcher_info_->watcher_cb &&
          !tmaster_location_watcher_info_->topology_name.empty());
}

bool HeronZKStateMgr::IsMetricsCacheWatchDefined() {
  return (metricscache_location_watcher_info_ != NULL &&
          metricscache_location_watcher_info_->watcher_cb &&
          !metricscache_location_watcher_info_->topology_name.empty());
}

// 2 seconds
const int HeronZKStateMgr::SET_WATCH_RETRY_INTERVAL_S = 2;

bool HeronZKStateMgr::ShouldRetrySetWatch(sp_int32 rc) {
  switch (rc) {
    case ZCONNECTIONLOSS:
    case ZOPERATIONTIMEOUT:
      return true;
    default:
      // Shouldn't retry for any other return code
      return false;
  }
}

void HeronZKStateMgr::SetTMasterWatchCompletionHandler(sp_int32 rc) {
  if (rc == ZOK || rc == ZNONODE) {
    // NoNode is when there is no tmaster up yet, but the watch is set.
    LOG(INFO) << "Setting watch on tmaster location succeeded: " << zerror(rc) << std::endl;
  } else {
    // Any other return code should be treated as warning, since ideally
    // we shouldn't be in this state.
    LOG(WARNING) << "Setting watch on tmaster location returned: " << zerror(rc) << std::endl;

    if (ShouldRetrySetWatch(rc)) {
      LOG(INFO) << "Retrying after " << SET_WATCH_RETRY_INTERVAL_S << " seconds" << std::endl;

      auto cb = [this](EventLoop::Status status) { this->CallSetTMasterLocationWatch(status); };

      eventLoop_->registerTimer(std::move(cb), false, SET_WATCH_RETRY_INTERVAL_S * 1000 * 1000);
    }
  }
}

void HeronZKStateMgr::SetMetricsCacheWatchCompletionHandler(sp_int32 rc) {
  if (rc == ZOK || rc == ZNONODE) {
    // NoNode is when there is no tmaster up yet, but the watch is set.
    LOG(INFO) << "Setting watch on metricscache location succeeded: " << zerror(rc) << std::endl;
  } else {
    // Any other return code should be treated as warning, since ideally
    // we shouldn't be in this state.
    LOG(WARNING) << "Setting watch on metricscache location returned: " << zerror(rc) << std::endl;

    if (ShouldRetrySetWatch(rc)) {
      LOG(INFO) << "Retrying after " << SET_WATCH_RETRY_INTERVAL_S << " seconds" << std::endl;

      auto cb = [this](EventLoop::Status status) { this->CallSetMetricsCacheLocationWatch(status);};

      eventLoop_->registerTimer(std::move(cb), false, SET_WATCH_RETRY_INTERVAL_S * 1000 * 1000);
    }
  }
}

void HeronZKStateMgr::CallSetTMasterLocationWatch(EventLoop::Status) {
  SetTMasterLocationWatchInternal();
}

void HeronZKStateMgr::CallSetMetricsCacheLocationWatch(EventLoop::Status) {
  SetMetricsCacheLocationWatchInternal();
}

void HeronZKStateMgr::SetTMasterLocationWatchInternal() {
  CHECK(IsTmasterWatchDefined());

  LOG(INFO) << "Setting watch on tmaster location " << std::endl;
  std::string path = GetTMasterLocationPath(tmaster_location_watcher_info_->topology_name);

  zkclient_->Exists(path, [this]() { this->TMasterLocationWatch(); },
                    [this](sp_int32 rc) { this->SetTMasterWatchCompletionHandler(rc); });
}

void HeronZKStateMgr::SetMetricsCacheLocationWatchInternal() {
  CHECK(IsMetricsCacheWatchDefined());

  LOG(INFO) << "Setting watch on metricscache location " << std::endl;
  std::string path = GetMetricsCacheLocationPath(
                    metricscache_location_watcher_info_->topology_name);

  zkclient_->Exists(path, [this]() { this->MetricsCacheLocationWatch(); },
                    [this](sp_int32 rc) { this->SetMetricsCacheWatchCompletionHandler(rc); });
}

void HeronZKStateMgr::TMasterLocationWatch() {
  // First setup watch again
  SetTMasterLocationWatchInternal();
  // Then run the watcher
  tmaster_location_watcher_info_->watcher_cb();
}

void HeronZKStateMgr::MetricsCacheLocationWatch() {
  // First setup watch again
  SetMetricsCacheLocationWatchInternal();
  // Then run the watcher
  metricscache_location_watcher_info_->watcher_cb();
}
}  // namespace common
}  // namespace heron
