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


#ifndef HERON_INSTANCE_EXECUTOR_TASK_CONTEXT_IMPL_H_
#define HERON_INSTANCE_EXECUTOR_TASK_CONTEXT_IMPL_H_

#include <map>
#include <list>
#include <string>
#include <utility>
#include <unordered_set>
#include "basics/basics.h"
#include "proto/messages.h"

#include "topology/task-context.h"
#include "tuple/fields.h"
#include "config/config.h"
#include "config/helper.h"

namespace heron {
namespace instance {

/**
 * This implements the TaskContext interface of the Heron API
 *
 */
class TaskContextImpl : public api::topology::TaskContext {
 public:
  explicit TaskContextImpl(int myTaskId);
  ~TaskContextImpl();
  void newPhysicalPlan(std::shared_ptr<proto::system::PhysicalPlan> pplan);

  // TopologyContext related implementations
  virtual const std::string& getTopologyId();
  virtual const std::string& getComponentName(int taskId);
  virtual void getComponentStreams(const std::string& componentName,
                                   std::unordered_set<std::string>& retval);
  virtual void getComponentTaskIds(const std::string& componentName,
                                   std::list<int>& retval);
  virtual api::tuple::Fields getComponentOutputFields(const std::string& componentName,
                                                 const std::string& streamId);
  virtual void getComponentSources(const std::string& componentName,
          std::map<std::pair<std::string, std::string>, proto::api::Grouping>& retval);
  virtual void getComponentTargets(const std::string& componentName,
           std::map<std::string, std::map<std::string, proto::api::Grouping>>& retval);
  virtual void getTaskIdToComponentName(std::map<int, std::string>& retval);
  virtual void getAllComponentNames(std::unordered_set<std::string>& retval);
  virtual std::shared_ptr<api::metric::IMetricsRegistrar> getMetricsRegistrar() {
    return metricsRegistrar_;
  }
  virtual void log(std::ostringstream& o);

  // TaskContext related implementations
  virtual int getThisTaskId();
  virtual const std::string& getThisComponentName();
  virtual api::tuple::Fields getThisOutputFields(const std::string& streamId);
  virtual void getThisStreams(std::unordered_set<std::string>& retval);
  virtual int getThisTaskIndex();
  virtual void getThisSources(std::map<std::pair<std::string, std::string>,
                                       proto::api::Grouping>& retval);
  virtual void getThisTargets(std::map<std::string,
                                       std::map<std::string, proto::api::Grouping>>& retval);
  virtual void setTaskData(const std::string& name, const std::string& value);
  virtual void getTaskData(const std::string& name, std::string& value);

  bool isSpout() const { return mySpout_ != NULL; }
  bool isAckingEnabled() const {
    return config::TopologyConfigHelper::GetReliabilityMode(pplan_->topology())
           == config::TopologyConfigVars::TopologyReliabilityMode::ATLEAST_ONCE;
  }
  bool enableMessageTimeouts() const {
    return config::TopologyConfigHelper::EnableMessageTimeouts(pplan_->topology());
  }
  const std::string& getComponentConstructor() const {
    return myComponent_->cpp_class_info().class_constructor();
  }
  std::shared_ptr<api::config::Config> getConfig() {
    return myMergedConfig_;
  }

  void setMericsRegistrar(std::shared_ptr<api::metric::IMetricsRegistrar> registrar) {
    metricsRegistrar_ = registrar;
  }

 private:
  void cleanUp();

  int myTaskId_;
  std::shared_ptr<proto::system::PhysicalPlan> pplan_;
  std::string myComponentName_;
  std::string myInstanceId_;
  std::map<std::string, int> myOutputSchema_;
  std::map<int, std::string> taskToComponentName_;
  proto::api::Component* myComponent_;
  proto::api::Spout* mySpout_;
  proto::api::Bolt* myBolt_;
  proto::system::Instance* myInstance_;
  std::shared_ptr<api::config::Config> myMergedConfig_;
  std::shared_ptr<api::metric::IMetricsRegistrar> metricsRegistrar_;
};

}  // namespace instance
}  // namespace heron

#endif
