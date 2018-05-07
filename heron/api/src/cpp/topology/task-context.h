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

#ifndef HERON_API_TOPOLOGY_TASK_CONTEXT_H_
#define HERON_API_TOPOLOGY_TASK_CONTEXT_H_

#include <map>
#include <unordered_set>
#include <string>
#include <utility>

#include "proto/messages.h"
#include "topology/topology-context.h"
#include "tuple/fields.h"

namespace heron {
namespace api {
namespace topology {

class TaskContext : public TopologyContext {
 public:
  /**
   * Gets the task id of this task.
   *
   * @return the task id
   */
  virtual int getThisTaskId() = 0;

  /**
   * Gets the component name for this task. The component name maps
   * to a component name specified for a Spout or Bolt in the topology definition.
   */
  virtual const std::string& getThisComponentName() = 0;

  /**
   * Gets the declared output fields for the specified stream id for the component
   * this task is running.
   */
  virtual tuple::Fields getThisOutputFields(const std::string& streamId) = 0;

  /**
   * Gets the set of streams declared for the component of this task.
   */
  virtual void getThisStreams(std::unordered_set<std::string>& retval) = 0;

  /**
   * Gets the index of this task id in getComponentTasks(getThisComponentId()).
   * An example use case for this method is determining which task
   * accesses which resource in a distributed resource to ensure an even distribution.
   */
  virtual int getThisTaskIndex() = 0;

  /**
   * Gets the declared inputs to this component.
   *
   * @return A map from subscribed component/stream to the grouping subscribed with.
   */
  virtual void getThisSources(std::map<std::pair<std::string, std::string>,
                                       proto::api::Grouping>& retval) = 0;

  /**
   * Gets information about who is consuming the outputs of this component, and how.
   *
   * @return Map from component name to (streamId, Grouping) map
   */
  virtual void getThisTargets(std::map<std::string,
                                       std::map<std::string, proto::api::Grouping>>& retval) = 0;

  virtual void setTaskData(const std::string& name, const std::string& value) = 0;

  virtual void getTaskData(const std::string& name, std::string& retval) = 0;
};

}  // namespace topology
}  // namespace api
}  // namespace heron

#endif  // HERON_API_TOPOLOGY_TASK_CONTEXT_H_
