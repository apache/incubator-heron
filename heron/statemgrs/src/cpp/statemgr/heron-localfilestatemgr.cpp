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

#include "statemgr/heron-localfilestatemgr.h"
#include <stdio.h>
#include <unistd.h>
#include <errno.h>
#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace common {

HeronLocalFileStateMgr::HeronLocalFileStateMgr(const std::string& _topleveldir,
                                               EventLoop* eventLoop)
    : HeronStateMgr(_topleveldir), eventLoop_(eventLoop) {
  InitTree();
}

HeronLocalFileStateMgr::~HeronLocalFileStateMgr() {
  // nothing really
}

void HeronLocalFileStateMgr::InitTree() {
  sp_string dpath = GetTopLevelDir();
  sp_string path = dpath;
  path += "/topologies";
  FileUtils::makeDirectory(path);
  path = dpath;
  path += "/tmasters";
  FileUtils::makeDirectory(path);
  path = dpath;
  path += "/pplans";
  FileUtils::makeDirectory(path);
  path = dpath;
  path += "/executionstate";
  FileUtils::makeDirectory(path);
}

void HeronLocalFileStateMgr::SetTMasterLocationWatch(const std::string& topology_name,
                                                     VCallback<> watcher) {
  CHECK(watcher);
  // We kind of cheat here. We check periodically
  time_t tmaster_last_change = FileUtils::getModifiedTime(GetTMasterLocationPath(topology_name));

  auto cb = [topology_name, tmaster_last_change, watcher, this](EventLoop::Status status) {
    this->CheckTMasterLocation(topology_name, tmaster_last_change, std::move(watcher), status);
  };

  CHECK_GT(eventLoop_->registerTimer(std::move(cb), false, 1000000), 0);
}

void HeronLocalFileStateMgr::SetMetricsCacheLocationWatch(const std::string& topology_name,
                                                     VCallback<> watcher) {
  CHECK(watcher);
  // We kind of cheat here. We check periodically
  time_t tmaster_last_change = FileUtils::getModifiedTime(
                               GetMetricsCacheLocationPath(topology_name));

  auto cb = [topology_name, tmaster_last_change, watcher, this](EventLoop::Status status) {
    this->CheckMetricsCacheLocation(topology_name, tmaster_last_change, std::move(watcher), status);
  };

  CHECK_GT(eventLoop_->registerTimer(std::move(cb), false, 1000000), 0);
}

void HeronLocalFileStateMgr::GetTMasterLocation(const std::string& _topology_name,
                                                proto::tmaster::TMasterLocation* _return,
                                                VCallback<proto::system::StatusCode> cb) {
  std::string contents;
  proto::system::StatusCode status =
      ReadAllFileContents(GetTMasterLocationPath(_topology_name), contents);
  if (status == proto::system::OK) {
    if (!_return->ParseFromString(contents)) {
      status = proto::system::STATE_CORRUPTED;
    }
  }

  auto wCb = [cb, status](EventLoop::Status) { cb(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
}

void HeronLocalFileStateMgr::GetMetricsCacheLocation(const std::string& _topology_name,
                                                proto::tmaster::MetricsCacheLocation* _return,
                                                VCallback<proto::system::StatusCode> cb) {
  std::string contents;
  proto::system::StatusCode status =
      ReadAllFileContents(GetMetricsCacheLocationPath(_topology_name), contents);
  if (status == proto::system::OK) {
    if (!_return->ParseFromString(contents)) {
      status = proto::system::STATE_CORRUPTED;
    }
  }

  auto wCb = [cb, status](EventLoop::Status) { cb(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
}

void HeronLocalFileStateMgr::SetTMasterLocation(const proto::tmaster::TMasterLocation& _location,
                                                VCallback<proto::system::StatusCode> cb) {
  // Note: Unlike Zk statemgr, we overwrite the location even if there is already one.
  // This is because when running in simulator we control when a tmaster dies and
  // comes up deterministically.
  std::string fname = GetTMasterLocationPath(_location.topology_name());
  std::string contents;
  _location.SerializeToString(&contents);
  proto::system::StatusCode status = WriteToFile(fname, contents);
  auto wCb = [cb, status](EventLoop::Status) { cb(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
}

void HeronLocalFileStateMgr::SetMetricsCacheLocation(
        const proto::tmaster::MetricsCacheLocation& _location,
        VCallback<proto::system::StatusCode> cb) {
  // Note: Unlike Zk statemgr, we overwrite the location even if there is already one.
  // This is because when running in simulator we control when a tmaster dies and
  // comes up deterministically.
  std::string fname = GetMetricsCacheLocationPath(_location.topology_name());
  std::string contents;
  _location.SerializeToString(&contents);
  proto::system::StatusCode status = WriteToFile(fname, contents);
  auto wCb = [cb, status](EventLoop::Status) { cb(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
}

void HeronLocalFileStateMgr::CreateTopology(const proto::api::Topology& _topology,
                                            VCallback<proto::system::StatusCode> cb) {
  std::string fname = GetTopologyPath(_topology.name());
  // First check to see if location exists.
  if (MakeSureFileDoesNotExist(fname) != proto::system::OK) {
    auto wCb = [cb](EventLoop::Status) { cb(proto::system::PATH_ALREADY_EXISTS); };
    CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
    return;
  }

  std::string contents;
  _topology.SerializeToString(&contents);
  proto::system::StatusCode status = WriteToFile(fname, contents);
  auto wCb = [cb, status](EventLoop::Status) { cb(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
}

void HeronLocalFileStateMgr::DeleteTopology(const sp_string& _topology_name,
                                            VCallback<proto::system::StatusCode> cb) {
  proto::system::StatusCode status = DeleteFile(GetTopologyPath(_topology_name));
  auto wCb = [cb, status](EventLoop::Status) { cb(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
}

void HeronLocalFileStateMgr::SetTopology(const proto::api::Topology& _topology,
                                         VCallback<proto::system::StatusCode> cb) {
  std::string fname = GetTopologyPath(_topology.name());
  std::string contents;
  _topology.SerializeToString(&contents);
  proto::system::StatusCode status = WriteToFile(fname, contents);
  auto wCb = [cb, status](EventLoop::Status) { cb(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
}

void HeronLocalFileStateMgr::GetTopology(const std::string& _topology_name,
                                         proto::api::Topology* _return,
                                         VCallback<proto::system::StatusCode> cb) {
  std::string contents;
  proto::system::StatusCode status = ReadAllFileContents(GetTopologyPath(_topology_name), contents);
  if (status == proto::system::OK) {
    if (!_return->ParseFromString(contents)) {
      status = proto::system::STATE_CORRUPTED;
    }
  }
  auto wCb = [cb, status](EventLoop::Status) { cb(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
}

void HeronLocalFileStateMgr::CreatePhysicalPlan(const proto::system::PhysicalPlan& _pplan,
                                                VCallback<proto::system::StatusCode> cb) {
  std::string fname = GetPhysicalPlanPath(_pplan.topology().name());
  // First check to see if location exists.
  if (MakeSureFileDoesNotExist(fname) != proto::system::OK) {
    auto wCb = [cb](EventLoop::Status) { cb(proto::system::PATH_ALREADY_EXISTS); };
    CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
    return;
  }

  std::string contents;
  _pplan.SerializeToString(&contents);
  proto::system::StatusCode status = WriteToFile(fname, contents);
  auto wCb = [cb, status](EventLoop::Status) { cb(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
}

void HeronLocalFileStateMgr::DeletePhysicalPlan(const sp_string& _topology_name,
                                                VCallback<proto::system::StatusCode> cb) {
  proto::system::StatusCode status = DeleteFile(GetPhysicalPlanPath(_topology_name));
  auto wCb = [cb, status](EventLoop::Status) { cb(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
}

void HeronLocalFileStateMgr::SetPhysicalPlan(const proto::system::PhysicalPlan& _pplan,
                                             VCallback<proto::system::StatusCode> cb) {
  std::string contents;
  _pplan.SerializeToString(&contents);
  proto::system::StatusCode status =
      WriteToFile(GetPhysicalPlanPath(_pplan.topology().name()), contents);
  auto wCb = [cb, status](EventLoop::Status) { cb(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
}

void HeronLocalFileStateMgr::GetPhysicalPlan(const std::string& _topology_name,
                                             proto::system::PhysicalPlan* _return,
                                             VCallback<proto::system::StatusCode> cb) {
  std::string contents;
  proto::system::StatusCode status =
      ReadAllFileContents(GetPhysicalPlanPath(_topology_name), contents);
  if (status == proto::system::OK) {
    if (!_return->ParseFromString(contents)) {
      status = proto::system::STATE_CORRUPTED;
    }
  }
  auto wCb = [cb, status](EventLoop::Status) { cb(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
}

void HeronLocalFileStateMgr::CreateExecutionState(const proto::system::ExecutionState& _st,
                                                  VCallback<proto::system::StatusCode> cb) {
  std::string fname = GetExecutionStatePath(_st.topology_name());
  // First check to see if location exists.
  if (MakeSureFileDoesNotExist(fname) != proto::system::OK) {
    auto wCb = [cb](EventLoop::Status) { cb(proto::system::PATH_ALREADY_EXISTS); };
    CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
    return;
  }

  std::string contents;
  _st.SerializeToString(&contents);
  proto::system::StatusCode status = WriteToFile(fname, contents);
  auto wCb = [cb, status](EventLoop::Status) { cb(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
}

void HeronLocalFileStateMgr::DeleteExecutionState(const std::string& _topology_name,
                                                  VCallback<proto::system::StatusCode> cb) {
  proto::system::StatusCode status = DeleteFile(GetExecutionStatePath(_topology_name));
  auto wCb = [cb, status](EventLoop::Status) { cb(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
}

void HeronLocalFileStateMgr::GetExecutionState(const std::string& _topology_name,
                                               proto::system::ExecutionState* _return,
                                               VCallback<proto::system::StatusCode> cb) {
  std::string contents;
  proto::system::StatusCode status =
      ReadAllFileContents(GetExecutionStatePath(_topology_name), contents);
  if (status == proto::system::OK) {
    if (!_return->ParseFromString(contents)) {
      status = proto::system::STATE_CORRUPTED;
    }
  }
  auto wCb = [cb, status](EventLoop::Status) { cb(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
}

void HeronLocalFileStateMgr::SetExecutionState(const proto::system::ExecutionState& _st,
                                               VCallback<proto::system::StatusCode> cb) {
  std::string fname = GetExecutionStatePath(_st.topology_name());
  std::string contents;
  _st.SerializeToString(&contents);
  proto::system::StatusCode status = WriteToFile(fname, contents);
  auto wCb = [cb, status](EventLoop::Status) { cb(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
}

void HeronLocalFileStateMgr::ListExecutionStateTopologies(std::vector<sp_string>* _return,
                                                          VCallback<proto::system::StatusCode> cb) {
  proto::system::StatusCode status = proto::system::OK;
  if (FileUtils::listFiles(GetExecutionStateDir(), *_return) != 0) {
    status = proto::system::NOTOK;
  }
  auto wCb = [cb, status](EventLoop::Status) { cb(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
}

void HeronLocalFileStateMgr::ListTopologies(std::vector<sp_string>* _return,
                                            VCallback<proto::system::StatusCode> cb) {
  proto::system::StatusCode status = proto::system::OK;
  if (FileUtils::listFiles(GetTopologyDir(), *_return) != 0) {
    status = proto::system::NOTOK;
  }
  auto wCb = [cb, status](EventLoop::Status) { cb(status); };
  CHECK_GT(eventLoop_->registerTimer(std::move(wCb), false, 0), 0);
}

proto::system::StatusCode HeronLocalFileStateMgr::ReadAllFileContents(const std::string& _filename,
                                                                      std::string& _contents) {
  std::ifstream in(_filename.c_str(), std::ios::in | std::ios::binary);
  if (in) {
    in.seekg(0, std::ios::end);
    _contents.resize(in.tellg());
    in.seekg(0, std::ios::beg);
    in.read(&_contents[0], _contents.size());
    in.close();
    return proto::system::OK;
  } else {
    // We could not open the file
    LOG(ERROR) << "Error reading from " << _filename << " with errno " << errno << "\n";
    return proto::system::PATH_DOES_NOT_EXIST;
  }
}

proto::system::StatusCode HeronLocalFileStateMgr::WriteToFile(const std::string& _filename,
                                                              const std::string& _contents) {
  const std::string tmp_filename = _filename + ".tmp";
  ::unlink(tmp_filename.c_str());
  std::ofstream ot(tmp_filename.c_str(), std::ios::out | std::ios::binary);
  if (ot) {
    ot << _contents;
    ot.close();
    if (rename(tmp_filename.c_str(), _filename.c_str()) != 0) {
      LOG(ERROR) << "Rename failed from " << tmp_filename << " to " << _filename << "\n";
      return proto::system::STATE_WRITE_ERROR;
    } else {
      return proto::system::OK;
    }
  } else {
    LOG(ERROR) << "Error writing to " << _filename << " with errno " << errno << std::endl;
    return proto::system::STATE_WRITE_ERROR;
  }
}

proto::system::StatusCode HeronLocalFileStateMgr::DeleteFile(const std::string& _filename) {
  if (remove(_filename.c_str()) != 0) {
    return proto::system::NOTOK;
  } else {
    return proto::system::OK;
  }
}

proto::system::StatusCode HeronLocalFileStateMgr::MakeSureFileDoesNotExist(
    const std::string& _filename) {
  std::ifstream in(_filename.c_str(), std::ios::in | std::ios::binary);
  if (in) {
    // it already exists.
    in.close();
    return proto::system::PATH_ALREADY_EXISTS;
  } else {
    return proto::system::OK;
  }
}

void HeronLocalFileStateMgr::CheckTMasterLocation(std::string topology_name, time_t last_change,
                                                  VCallback<> watcher, EventLoop::Status) {
  time_t nlast_change = FileUtils::getModifiedTime(GetTMasterLocationPath(topology_name));
  if (nlast_change > last_change) {
    watcher();
  } else {
    nlast_change = last_change;
  }

  auto cb = [topology_name, nlast_change, watcher, this](EventLoop::Status status) {
    this->CheckTMasterLocation(topology_name, nlast_change, std::move(watcher), status);
  };

  CHECK_GT(eventLoop_->registerTimer(std::move(cb), false, 1000000), 0);
}

void HeronLocalFileStateMgr::CheckMetricsCacheLocation(
        std::string topology_name, time_t last_change,
        VCallback<> watcher, EventLoop::Status) {
  time_t nlast_change = FileUtils::getModifiedTime(GetMetricsCacheLocationPath(topology_name));
  if (nlast_change > last_change) {
    watcher();
  } else {
    nlast_change = last_change;
  }

  auto cb = [topology_name, nlast_change, watcher, this](EventLoop::Status status) {
    this->CheckMetricsCacheLocation(topology_name, nlast_change, std::move(watcher), status);
  };

  CHECK_GT(eventLoop_->registerTimer(std::move(cb), false, 1000000), 0);
}

}  // namespace common
}  // namespace heron
