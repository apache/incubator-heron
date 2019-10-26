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

#ifndef HERON_API_TOPOLOGY_ICOMPONENT_H_
#define HERON_API_TOPOLOGY_ICOMPONENT_H_

#include <string>
#include <memory>

#include "config/config.h"
#include "topology/output-fields-declarer.h"

namespace heron {
namespace api {
namespace topology {

class IComponent {
 public:
  /**
   * Declare the output schema for all the streams of this topology.
   *
   * @param declarer this is used to declare output stream ids, output fields,
   * and whether or not each output stream is a direct stream
   */
  virtual void declareOutputFields(std::shared_ptr<OutputFieldsDeclarer> declarer) = 0;

  /**
   * Declare configuration specific to this component. Only a subset of the "topology.*" configs can
   * be overridden. The component configuration can be further overridden when constructing the
   * topology using {@link TopologyBuilder}
   */
  virtual std::shared_ptr<config::Config> getComponentConfiguration() = 0;
};

}  // namespace topology
}  // namespace api
}  // namespace heron

#endif  // HERON_API_TOPOLOGY_ICOMPONENT_H_
