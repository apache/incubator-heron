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

#ifndef HERON_API_TOPOLOGY_TOPOLOGY_BUILDER_H_
#define HERON_API_TOPOLOGY_TOPOLOGY_BUILDER_H_

#include <stdlib.h>
#include <time.h>

#include <map>
#include <string>

#include "proto/messages.h"
#include "bolt/irich-bolt.h"
#include "spout/irich-spout.h"
#include "config/config.h"
#include "topology/bolt-declarer.h"
#include "topology/spout-declarer.h"

namespace heron {
namespace api {
namespace topology {

/**
 * TopologyBuilder exposes the C++ API for specifying a topology for Heron
 * to execute. TopologyBuilder greatly eases the process of creating topologies.
 * The template for creating and submitting a topology looks something like:
 * <p>
 * <pre>
 * auto builder = std::make_shared<TopologyBuilder>();
 *
 * builder->setSpout("1", std::make_shared<TestWordSpout>(true), "createTestWordSpout", 5);
 * builder->setSpout("2", std::make_shared<TestWordSpout>(true), "createTestWordSpout", 3);
 * builder->setBolt("3", std::make_shared<TestWordCounter>(), "createTestWordCounter", 3)
 *          ->fieldsGrouping("1", std::make_shared<Fields>("word"))
 *          ->fieldsGrouping("2", std::make_shared<Fields>("word"));
 * builder->setBolt("4", std::make_shared<TestGlobalCount>(), "createTestGlobalCount")
 *          ->globalGrouping("1");
 *
 * auto conf = std::make_shared<Config>();
 * conf->setStMgrs(4);
 *
 * HeronSubmitter::submitTopology(builder->createTopology("mytopology", conf));
 * <p>The pattern for TopologyBuilder is to map component ids to components using the setSpout
 * and setBolt methods. Those methods return objects that are then used to declare
 * the inputs for that component.</p>
 */
class TopologyBuilder {
 public:
  TopologyBuilder() : topology_(nullptr) { }
  ~TopologyBuilder() { delete topology_; }

  const proto::api::Topology& createTopology(const std::string& name,
                                             std::shared_ptr<config::Config> userConfig) {
    if (name.empty()) {
      throw std::invalid_argument("Failed to build topology; missing necessary info");
    }

    topology_ = new proto::api::Topology();

    srand(time(nullptr));
    std::ostringstream ss;
    ss << rand();
    std::string topologyId = name + ss.str();

    topology_->set_name(name);
    topology_->set_id(topologyId);
    topology_->set_state(proto::api::TopologyState::RUNNING);

    addDefaultTopologyConfig(userConfig);
    userConfig->setTopologyName(name);
    userConfig->dump(topology_->mutable_topology_config());

    for (auto& kv : spouts_) {
      kv.second->dump(topology_);
    }
    for (auto& kv : bolts_) {
      kv.second->dump(topology_);
    }
    return *topology_;
  }

  /**
   * Define a new bolt in this topology with parallelism of one.
   *
   * @param name the name of this component. This id is referenced by other components
   * that want to consume this bolt's outputs.
   * @param bolt the bolt
   * @param constructor the extern "C" style function that can be used to create the bolt
   * @return use the returned object to declare the inputs to this component
   */
  std::shared_ptr<topology::BoltDeclarer> setBolt(const std::string& name,
                                                  std::shared_ptr<bolt::IRichBolt> bolt,
                                                  const std::string& constructor) {
    return setBolt(name, bolt, constructor, 1);
  }

  /**
   * Define a new bolt in this topology with the specified amount of parallelism.
   *
   * @param name the name of this component. This id is referenced by other components
   * that want to consume this bolt's outputs.
   * @param bolt the bolt
   * @param constructor the extern "C" style function that can be used to create the bolt
   * @param parallelismHint the number of tasks that should be assigned to execute this bolt.
   * Each task will run in a seperate process somewhere around the cluster.
   * @return use the returned object to declare the inputs to this component
   */
  std::shared_ptr<topology::BoltDeclarer> setBolt(const std::string& name,
                                                  std::shared_ptr<bolt::IRichBolt> bolt,
                                                  const std::string& constructor,
                                                  int parallelism) {
    validateComponentName(name);
    auto b = std::make_shared<topology::BoltDeclarer>(name, constructor, bolt, parallelism);
    bolts_[name] = b;
    return b;
  }

  /**
   * Define a new spout in this topology.
   *
   * @param name the name of this component. This id is referenced by other components
   * that want to consume this spout's outputs.
   * @param spout the spout
   * @param constructor the extern "C" style function that can be used to create the spout
   */
  std::shared_ptr<SpoutDeclarer> setSpout(const std::string& name,
                                          std::shared_ptr<spout::IRichSpout> spout,
                                          const std::string& constructor) {
    return setSpout(name, spout, constructor, 1);
  }

  /**
   * Define a new spout in this topology.
   *
   * @param name the name of this component. This id is referenced by other components
   * that want to consume this spout's outputs.
   * @param spout the spout
   * @param constructor the extern "C" style function that can be used to create the spout
   * @param parallelismHint the number of tasks that should be assigned to execute this spout.
   * Each task will run in a seperate process somewhere around the cluster
   */
  std::shared_ptr<SpoutDeclarer> setSpout(const std::string& name,
                                          std::shared_ptr<spout::IRichSpout> spout,
                                          const std::string& constructor,
                                          int parallelismHint) {
    validateComponentName(name);
    auto b = std::make_shared<topology::SpoutDeclarer>(name, constructor,
                                                       spout, parallelismHint);
    spouts_[name] = b;
    return b;
  }

 private:
  void validateComponentName(const std::string& componentName) {
    if (componentName.find(',') != std::string::npos) {
      throw std::invalid_argument("Component name should not contain comma(,)");
    }
    if (componentName.find(':') != std::string::npos) {
      throw std::invalid_argument("Component name should not contain colon(:)");
    }
    validateUnusedName(componentName);
  }

  void validateUnusedName(const std::string& componentName) {
    if (spouts_.find(componentName) != spouts_.end()) {
      throw std::invalid_argument("A Spout has already been declared in the same name");
    }
    if (bolts_.find(componentName) != bolts_.end()) {
      throw std::invalid_argument("A Bolt has already been declared in the same name");
    }
  }

  void addDefaultTopologyConfig(std::shared_ptr<config::Config> userConfig) {
    if (!userConfig->hasConfig(config::Config::TOPOLOGY_DEBUG)) {
      userConfig->setDebug(false);
    }
    if (!userConfig->hasConfig(config::Config::TOPOLOGY_STMGRS)) {
      userConfig->setNumStmgrs(1);
    }
    if (!userConfig->hasConfig(config::Config::TOPOLOGY_MESSAGE_TIMEOUT_SECS)) {
      userConfig->setMessageTimeoutSecs(30);
    }
    if (!userConfig->hasConfig(config::Config::TOPOLOGY_COMPONENT_PARALLELISM)) {
      userConfig->setComponentParallelism(1);
    }
    if (!userConfig->hasConfig(config::Config::TOPOLOGY_MAX_SPOUT_PENDING)) {
      userConfig->setMaxSpoutPending(100);
    }
    if (!userConfig->hasConfig(config::Config::TOPOLOGY_RELIABILITY_MODE)) {
      userConfig->setTopologyReliabilityMode(config::Config::ATMOST_ONCE);
    }
    if (!userConfig->hasConfig(config::Config::TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS)) {
      userConfig->setEnableMessageTimeouts(true);
    }
  }

  std::map<std::string, std::shared_ptr<BoltDeclarer>> bolts_;
  std::map<std::string, std::shared_ptr<SpoutDeclarer>> spouts_;
  proto::api::Topology* topology_;
};

}  // namespace topology
}  // namespace api
}  // namespace heron

#endif  // HERON_API_TOPOLOGY_TOPOLOGY_BUILDER_H_
