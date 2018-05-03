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

#ifndef HERON_API_TOPOLOGY_SPOUT_DECLARER_H_
#define HERON_API_TOPOLOGY_SPOUT_DECLARER_H_

#include <map>
#include <string>

#include "proto/messages.h"

#include "config/config.h"
#include "spout/irich-spout.h"
#include "topology/output-fields-getter.h"
#include "topology/base-component-declarer.h"

namespace heron {
namespace api {
namespace topology {

class SpoutDeclarer : public BaseComponentDeclarer<SpoutDeclarer> {
 public:
  SpoutDeclarer(const std::string& spoutName,
                const std::string& spoutConstructorFunction,
                std::shared_ptr<spout::IRichSpout> spout, int taskParallelism)
    : BaseComponentDeclarer<SpoutDeclarer>(spoutName, spoutConstructorFunction,
                                           spout, taskParallelism) {
    output_.reset(new OutputFieldsGetter());
    spout->declareOutputFields(output_);
  }

  virtual ~SpoutDeclarer() { }

  SpoutDeclarer* setMaxSpoutPending(int maxSpoutPending) {
    addConfiguration(config::Config::TOPOLOGY_MAX_SPOUT_PENDING, std::to_string(maxSpoutPending));
    return returnThis();
  }

  virtual SpoutDeclarer* returnThis() {
    return this;
  }

  void dump(proto::api::Topology* topology) {
    proto::api::Spout* spout = topology->add_spouts();
    BaseComponentDeclarer<SpoutDeclarer>::dump(spout->mutable_comp());
    output_->dump(spout);
  }

 private:
  std::shared_ptr<OutputFieldsGetter> output_;
};

}  // namespace topology
}  // namespace api
}  // namespace heron

#endif  // HERON_API_TOPOLOGY_SPOUT_DECLARER_H_
