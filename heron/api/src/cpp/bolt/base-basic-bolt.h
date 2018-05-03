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

#ifndef HERON_API_BOLT_BASE_BASIC_BOLT_H_
#define HERON_API_BOLT_BASE_BASIC_BOLT_H_

#include <string>
#include <memory>

#include "config/config.h"
#include "bolt/ibasic-bolt.h"

namespace heron {
namespace api {
namespace bolt {

class BaseBasicBolt : public IBasicBolt {
 public:
  /**
   * Called when a task for this component is initialized on the cluster.
   * It provides the bolt with the environment in which the bolt executes.
   * <p>
   * <p>This includes the:</p>
   *
   * @param conf The Heron configuration for this bolt. This is the configuration provided
   * to the topology merged in with cluster configuration on this machine.
   * @param context This object can be used to get information about this task's place within
   * the topology, including the task id and component id of this task, input and output
   * information, etc.
   */
  virtual void prepare(std::shared_ptr<config::Config> conf,
                       std::shared_ptr<topology::TaskContext> context) {
  }

  virtual void cleanup() {
  }

  virtual std::shared_ptr<config::Config> getComponentConfiguration() {
    return nullptr;
  }
};

}  // namespace bolt
}  // namespace api
}  // namespace heron

#endif  // HERON_API_BOLT_BASE_BASIC_BOLT_H_
