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

#ifndef HERON_API_TOPOLOGY_TOPOLOGY_CONTEXT_H_
#define HERON_API_TOPOLOGY_TOPOLOGY_CONTEXT_H_

#include <list>
#include <map>
#include <unordered_set>
#include <string>
#include <utility>

#include "proto/messages.h"
#include "metric/imetric.h"
#include "metric/imetrics-registrar.h"
#include "tuple/fields.h"

namespace heron {
namespace api {
namespace topology {

class TopologyContext {
 public:
  /**
   * Gets the unique id assigned to this topology. The id is the topology name with a
   * unique nonce appended to it.
   *
   * @return the topology id
   */
  virtual const std::string& getTopologyId() = 0;

  /**
   * Gets the component name for the specified task id. The component id maps
   * to a component id specified for a Spout or Bolt in the topology definition.
   *
   * @param taskId the task id
   * @return the component id for the input task id
   */
  virtual const std::string& getComponentName(int taskId) = 0;

  /**
   * Gets the set of streams declared for the specified component.
   */
  virtual void getComponentStreams(const std::string& componentName,
                                   std::unordered_set<std::string>& retval) = 0;

  /**
   * Gets the task ids allocated for the given component id. The task ids are
   * always returned in ascending order.
   */
  virtual void getComponentTaskIds(const std::string& componentName,
                                   std::list<int>& retval) = 0;

  /**
   * Gets the declared output fields for the specified component/stream.
   */
  virtual tuple::Fields getComponentOutputFields(const std::string& componentName,
                                                 const std::string& streamId) = 0;

  /**
   * Gets the declared inputs to the specified component.
   *
   * @return A map from subscribed component/stream to the grouping subscribed with.
   */
  virtual void getComponentSources(const std::string& componentName,
          std::map<std::pair<std::string, std::string>, proto::api::Grouping>& retval) = 0;

  /**
   * Gets information about who is consuming the outputs of the specified component,
   * and how.
   *
   * @return map from component name to (streamId, Grouping) pair
   */
  virtual void getComponentTargets(const std::string& componentName,
           std::map<std::string, std::map<std::string, proto::api::Grouping>>& retval) = 0;

  /**
   * Gets a map from task id to component name.
   */
  virtual void getTaskIdToComponentName(std::map<int, std::string>& retval) = 0;

  /**
   * Gets a list of all component ids in this topology
   */
  virtual void getAllComponentNames(std::unordered_set<std::string>& retval) = 0;

  /**
   * Gets the metrics registrar with whom you can register metrics
   */
  virtual std::shared_ptr<metric::IMetricsRegistrar> getMetricsRegistrar() = 0;

  /**
   * Log the contents of the ostream
   */
  virtual void log(std::ostringstream& ostr) = 0;
};

}  // namespace topology
}  // namespace api
}  // namespace heron

#endif  // HERON_API_TOPOLOGY_TOPOLOGY_CONTEXT_H_
