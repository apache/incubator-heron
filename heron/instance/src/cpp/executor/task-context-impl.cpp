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

#include <map>
#include <list>
#include <unordered_set>
#include <string>
#include <utility>
#include "executor/task-context-impl.h"
#include "basics/basics.h"
#include "proto/messages.h"
#include "network/network.h"
#include "threads/threads.h"

namespace heron {
namespace instance {

TaskContextImpl::TaskContextImpl(int myTaskId)
  : myTaskId_(myTaskId), pplan_(nullptr), myComponent_(NULL),
    mySpout_(NULL), myBolt_(NULL), myInstance_(NULL),
    myMergedConfig_(new api::config::Config()) {
}

TaskContextImpl::~TaskContextImpl() {}

void TaskContextImpl::newPhysicalPlan(std::shared_ptr<proto::system::PhysicalPlan> pplan) {
  cleanUp();
  pplan_ = pplan;
  for (int i = 0; i < pplan_->instances_size(); ++i) {
    if (pplan_->instances(i).info().task_id() == myTaskId_) {
      myInstance_ = pplan_->mutable_instances(i);
      break;
    }
  }
  if (!myInstance_) {
    LOG(FATAL) << "There was no instance that matched my id " << myTaskId_;
  }
  myComponentName_ = myInstance_->info().component_name();
  myInstanceId_ = myInstance_->instance_id();
  for (int i = 0; i < pplan_->topology().spouts_size(); ++i) {
    if (pplan_->topology().spouts(i).comp().name() == myComponentName_) {
      mySpout_ = pplan_->mutable_topology()->mutable_spouts(i);
      myComponent_ = mySpout_->mutable_comp();
      break;
    }
  }
  for (int i = 0; i < pplan_->topology().bolts_size(); ++i) {
    if (pplan_->topology().bolts(i).comp().name() == myComponentName_) {
      myBolt_ = pplan_->mutable_topology()->mutable_bolts(i);
      myComponent_ = myBolt_->mutable_comp();
      break;
    }
  }
  if (!mySpout_ && !myBolt_) {
    LOG(FATAL) << myComponentName_ << " is neither a spout nor a bolt";
  }
  if (mySpout_ && myBolt_) {
    LOG(FATAL) << myComponentName_ << " is bolt a spout and a bolt";
  }
  if (mySpout_) {
    for (auto output : mySpout_->outputs()) {
      myOutputSchema_[output.stream().id()] = output.schema().keys_size();
    }
  } else {
    for (auto output : myBolt_->outputs()) {
      myOutputSchema_[output.stream().id()] = output.schema().keys_size();
    }
  }
  myMergedConfig_->insert(pplan_->topology().topology_config());
  myMergedConfig_->insert(myComponent_->config());
}

void TaskContextImpl::cleanUp() {
  if (pplan_) {
    pplan_ = nullptr;
  }
  myOutputSchema_.clear();
  taskToComponentName_.clear();
  myComponent_ = NULL;
  mySpout_ = NULL;
  myBolt_ = NULL;
  myInstance_ = NULL;
  myMergedConfig_->clear();
}

const std::string& TaskContextImpl::getTopologyId() {
  return pplan_->topology().id();
}

const std::string& TaskContextImpl::getComponentName(int taskId) {
  return config::PhysicalPlanHelper::GetComponentName(*pplan_, taskId);
}

void TaskContextImpl::getComponentStreams(const std::string& componentName,
                                          std::unordered_set<std::string>& retval) {
  config::TopologyConfigHelper::GetComponentStreams(pplan_->topology(), componentName, retval);
}

void TaskContextImpl::getComponentTaskIds(const std::string& componentName,
                                          std::list<int>& retval) {
  config::PhysicalPlanHelper::GetComponentTaskIds(*pplan_, componentName, retval);
}

api::tuple::Fields TaskContextImpl::getComponentOutputFields(const std::string& componentName,
                                                        const std::string& streamId) {
  proto::api::StreamSchema* schema =
              config::TopologyConfigHelper::GetStreamSchema(*(pplan_->mutable_topology()),
                                                            componentName,
                                                            streamId);
  std::list<std::string> keys;
  if (schema) {
    for (auto name : schema->keys()) {
      keys.push_back(name.key());
    }
  }
  return api::tuple::Fields(keys);
}

void TaskContextImpl::getComponentSources(const std::string& componentName,
          std::map<std::pair<std::string, std::string>, proto::api::Grouping>& retval) {
  config::TopologyConfigHelper::GetComponentSources(pplan_->topology(), componentName, retval);
}

void TaskContextImpl::getComponentTargets(const std::string& componentName,
           std::map<std::string, std::map<std::string, proto::api::Grouping>>& retval) {
  config::TopologyConfigHelper::GetComponentTargets(pplan_->topology(), componentName, retval);
}

void TaskContextImpl::getTaskIdToComponentName(std::map<int, std::string>& retval) {
  config::PhysicalPlanHelper::GetTaskIdToComponentName(*pplan_, retval);
}

void TaskContextImpl::getAllComponentNames(std::unordered_set<std::string>& retval) {
  config::TopologyConfigHelper::GetAllComponentNames(pplan_->topology(), retval);
}

void TaskContextImpl::log(std::ostringstream& ostr) {
  LOG(INFO) << ostr.str();
  ostr.str(std::string());  // reset it
}

int TaskContextImpl::getThisTaskId() {
  return myTaskId_;
}

const std::string& TaskContextImpl::getThisComponentName() {
  return myComponent_->name();
}

api::tuple::Fields TaskContextImpl::getThisOutputFields(const std::string& streamId) {
  return getComponentOutputFields(getThisComponentName(), streamId);
}

void TaskContextImpl::getThisStreams(std::unordered_set<std::string>& retval) {
  return getComponentStreams(getThisComponentName(), retval);
}

int TaskContextImpl::getThisTaskIndex() {
  return myInstance_->info().component_index();
}

void TaskContextImpl::getThisSources(std::map<std::pair<std::string, std::string>,
                                              proto::api::Grouping>& retval) {
  return getComponentSources(getThisComponentName(), retval);
}

void TaskContextImpl::getThisTargets(std::map<std::string,
                                            std::map<std::string, proto::api::Grouping>>& retval) {
  return getComponentTargets(getThisComponentName(), retval);
}

void TaskContextImpl::setTaskData(const std::string& name, const std::string& value) {
  myMergedConfig_->insert(name, value);
}

void TaskContextImpl::getTaskData(const std::string& name, std::string& value) {
  if (myMergedConfig_->hasConfig(name)) {
    value = myMergedConfig_->get(name);
  }
}

}  // namespace instance
}  // namespace heron
