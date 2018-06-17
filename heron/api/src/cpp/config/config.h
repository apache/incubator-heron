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

#ifndef HERON_API_CONFIG_CONFIG_H_
#define HERON_API_CONFIG_CONFIG_H_

#include <map>
#include <set>
#include <string>
#include <sstream>

#include "proto/messages.h"

namespace heron {
namespace api {
namespace config {

/**
 * Topology configs are specified as a plain old map. This class provides a
 * convenient way to create a topology config map by providing setter methods for
 * all the configs that can be set. It also makes it easier to do things like add
 * serializations.
 * <p>
 * <p>Note that you may put other configurations in any of the configs. Heron
 * will ignore anything it doesn't recognize, but your topologies are free to make
 * use of them by reading them in the prepare method of Bolts or the open method of
 * Spouts.
 */
class Config {
 public:
  /**
   * How often a tick tuple from the "__system" component and "__tick" stream should be sent
   * to tasks. Meant to be used as a component-specific configuration.
   */
  static const std::string TOPOLOGY_TICK_TUPLE_FREQ_SECS;

  /**
   * True if Heron should timeout messages or not. Defaults to true. This is meant to be used
   * in unit tests to prevent tuples from being accidentally timed out during the test.
   */
  static const std::string TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS;

  /**
   * When set to true, Heron will log every message that's emitted.
   */
  static const std::string TOPOLOGY_DEBUG;

  /**
   * The number of stmgr instances that should spin up to service this
   * topology. All the executors will be evenly shared by these stmgrs.
   */
  static const std::string TOPOLOGY_STMGRS;

  /**
   * The maximum amount of time given to the topology to fully process a message
   * emitted by a spout. If the message is not acked within this time frame, Heron
   * will fail the message on the spout. Some spouts implementations will then replay
   * the message at a later time.
   */
  static const std::string TOPOLOGY_MESSAGE_TIMEOUT_SECS;

  /**
   * The per component parallelism for a component in this topology.
   * Note:- If you are changing this, please change the utils.h as well
   */
  static const std::string TOPOLOGY_COMPONENT_PARALLELISM;

  /**
   * The maximum number of tuples that can be pending on a spout task at any given time.
   * This config applies to individual tasks, not to spouts or topologies as a whole.
   * <p>
   * A pending tuple is one that has been emitted from a spout but has not been acked or failed yet.
   * Note that this config parameter has no effect for unreliable spouts that don't tag
   * their tuples with a message id.
   */
  static const std::string TOPOLOGY_MAX_SPOUT_PENDING;

  /**
   * What is the reliability mode under which we are running this topology
   * Topology writers must set TOPOLOGY_RELIABILITY_MODE to one
   * one of the following modes
   */
  enum TopologyReliabilityMode {
    /**
     * Heron provides no guarantees wrt tuple delivery. Tuples emitted by
     * components can get lost for any reason(network issues, component failures,
     * overloaded downstream component, etc).
     */
    ATMOST_ONCE,
    /**
     * Heron guarantees that each emitted tuple is seen by the downstream components
     * atleast once. This is achieved via the anchoring process where emitted tuples
     * are anchored based on input tuples. Note that in failure scenarios, downstream
     * components can see the same tuple multiple times.
     */
    ATLEAST_ONCE,
    /**
     * Heron guarantees that each emitted tuple is seen by the downstream components
     * effectively once. This is achieved via distributed snapshotting approach is described at
     * https://docs.google.com/document/d/1pNuE77diSrYHb7vHPuPO3DZqYdcxrhywH_f7loVryCI/edit
     * In this mode Heron will try to take the snapshots of
     * all of the components of the topology every
     * TOPOLOGY_STATEFUL_CHECKPOINT_INTERVAL_SECONDS seconds. Upon failure of
     * any component or detection of any network failure, Heron will initiate a recovery
     * mechanism to revert the topology to the last globally consistent checkpoint
     */
    EFFECTIVELY_ONCE
  };

  /**
   * A Heron topology can be run in any one of the TopologyReliabilityMode
   * mode. The format of this flag is the string encoded values of the
   * underlying TopologyReliabilityMode value.
   */
  static const std::string TOPOLOGY_RELIABILITY_MODE;

  /**
   * Number of CPU cores per container to be reserved for this topology
   */
  static const std::string TOPOLOGY_CONTAINER_CPU_REQUESTED;

  /**
   * Amount of RAM per container to be reserved for this topology.
   * In bytes.
   */
  static const std::string TOPOLOGY_CONTAINER_RAM_REQUESTED;

  /**
   * Amount of disk per container to be reserved for this topology.
   * In bytes.
   */
  static const std::string TOPOLOGY_CONTAINER_DISK_REQUESTED;

  /**
   * Hint for max number of CPU cores per container to be reserved for this topology
   */
  static const std::string TOPOLOGY_CONTAINER_MAX_CPU_HINT;

  /**
   * Hint for max amount of RAM per container to be reserved for this topology.
   * In bytes.
   */
  static const std::string TOPOLOGY_CONTAINER_MAX_RAM_HINT;

  /**
   * Hint for max amount of disk per container to be reserved for this topology.
   * In bytes.
   */
  static const std::string TOPOLOGY_CONTAINER_MAX_DISK_HINT;

  /**
   * Hint for max amount of disk per container to be reserved for this topology.
   * In bytes.
   */
  static const std::string TOPOLOGY_CONTAINER_PADDING_PERCENTAGE;

  /**
   * Amount of RAM to pad each container for this topology.
   * In bytes.
   */
  static const std::string TOPOLOGY_CONTAINER_RAM_PADDING;

  /**
   * Per component CPU requirement.  The format of this flag is something like
   * spout0:0.2,spout1:0.2,bolt1:0.5.
   */
  static const std::string TOPOLOGY_COMPONENT_CPUMAP;

  /**
   * Per component RAM requirement.  The format of this flag is something like
   * spout0:12434,spout1:345353,bolt1:545356.
   */
  static const std::string TOPOLOGY_COMPONENT_RAMMAP;

  /**
   * Per component disk requirement.  The format of this flag is something like
   * spout0:12434,spout1:345353,bolt1:545356.
   */
  static const std::string TOPOLOGY_COMPONENT_DISKMAP;

  /**
   * Name of the serializer classname. Only 'cereal', or 'string' are supported
   */
  static const std::string TOPOLOGY_SERIALIZER_CLASSNAME;

  /**
   * Name of the topology. This config is automatically set by Heron when the topology is submitted.
   */
  static const std::string TOPOLOGY_NAME;

  Config() { }
  explicit Config(const std::map<std::string, std::string>& conf) {
    config_.insert(conf.begin(), conf.end());
  }

  void setDebug(bool debug) {
    config_[Config::TOPOLOGY_DEBUG] = debug ? "true" : "false";
  }

  void setNumStmgrs(int nStmgrs) {
    config_[Config::TOPOLOGY_STMGRS] = std::to_string(nStmgrs);
  }

  void setTopologyReliabilityMode(Config::TopologyReliabilityMode mode);

  void setMessageTimeoutSecs(int seconds) {
    config_[Config::TOPOLOGY_MESSAGE_TIMEOUT_SECS] = std::to_string(seconds);
  }

  void setEnableMessageTimeouts(bool enable) {
    config_[Config::TOPOLOGY_ENABLE_MESSAGE_TIMEOUTS] = enable ? "true" : "false";
  }

  void setComponentParallelism(int parallelism) {
    config_[Config::TOPOLOGY_COMPONENT_PARALLELISM] = std::to_string(parallelism);
  }

  void setMaxSpoutPending(int pending) {
    config_[Config::TOPOLOGY_MAX_SPOUT_PENDING] = std::to_string(pending);
  }

  void setTickTupleFrequency(int seconds) {
    config_[Config::TOPOLOGY_TICK_TUPLE_FREQ_SECS] = std::to_string(seconds);
  }

  void setContainerCpuRequested(double ncpus) {
    config_[Config::TOPOLOGY_CONTAINER_CPU_REQUESTED] = std::to_string(ncpus);
  }

  void setContainerDiskRequested(int64_t bytes) {
    config_[Config::TOPOLOGY_CONTAINER_DISK_REQUESTED] = std::to_string(bytes);
  }

  void setContainerRamRequested(int64_t bytes) {
    config_[Config::TOPOLOGY_CONTAINER_RAM_REQUESTED] = std::to_string(bytes);
  }

  void setContainerMaxCpuHint(double ncpus) {
    config_[Config::TOPOLOGY_CONTAINER_MAX_CPU_HINT] = std::to_string(ncpus);
  }

  void setContainerMaxDiskHint(int64_t bytes) {
    config_[Config::TOPOLOGY_CONTAINER_MAX_DISK_HINT] = std::to_string(bytes);
  }

  void setContainerMaxRamHint(int64_t bytes) {
    config_[Config::TOPOLOGY_CONTAINER_MAX_RAM_HINT] = std::to_string(bytes);
  }

  void setContainerPaddingPercentage(int percentage) {
    config_[Config::TOPOLOGY_CONTAINER_PADDING_PERCENTAGE] = std::to_string(percentage);
  }

  void setContainerRamPadding(int64_t bytes) {
    config_[Config::TOPOLOGY_CONTAINER_RAM_PADDING] = std::to_string(bytes);
  }

  void setComponentCpuMap(const std::string& cpumap) {
    config_[Config::TOPOLOGY_COMPONENT_CPUMAP] = cpumap;
  }

  void setComponentRamMap(const std::string& rammap) {
    config_[Config::TOPOLOGY_COMPONENT_RAMMAP] = rammap;
  }

  void setComponentDiskMap(const std::string& diskmap) {
    config_[Config::TOPOLOGY_COMPONENT_DISKMAP] = diskmap;
  }

  void setSerializerClassName(const std::string& className) {
    config_[Config::TOPOLOGY_SERIALIZER_CLASSNAME] = className;
  }

  template<typename T>
  void appendComponentConfig(const std::string& key,
                             const std::string& componentName,
                             const T& val) {
    if (config_.find(key) != config_.end()) {
      std::ostringstream value;
      value << config_[key];
      value << "," << componentName << ":" << val;
      config_[key] = value.str();
    } else {
      std::ostringstream value;
      value << componentName << ":" << val;
      config_[key] = value.str();
    }
  }

  void setComponentCpu(const std::string& componentName, double cpu) {
    if (cpu < 0) {
      throw std::runtime_error("Invalid CPU specified for component");
    }
    appendComponentConfig<double>(Config::TOPOLOGY_COMPONENT_CPUMAP, componentName, cpu);
  }

  void setComponentRam(const std::string& componentName, int64_t bytes) {
    if (bytes < 0) {
      throw std::runtime_error("Invalid RAM specified for component");
    }
    appendComponentConfig<int64_t>(Config::TOPOLOGY_COMPONENT_RAMMAP, componentName, bytes);
  }

  void setComponentDisk(const std::string& componentName, int64_t bytes) {
    if (bytes < 0) {
      throw std::runtime_error("Invalid Disk specified for component");
    }
    appendComponentConfig<int64_t>(Config::TOPOLOGY_COMPONENT_DISKMAP, componentName, bytes);
  }

  bool hasConfig(const std::string& name) {
    return config_.find(name) != config_.end();
  }

  const std::string& get(const std::string& name) {
    return config_[name];
  }

  void setTopologyName(const std::string& name) {
    config_[Config::TOPOLOGY_NAME] = name;
  }

  void insert(const std::map<std::string, std::string>& conf) {
    config_.insert(conf.begin(), conf.end());
  }

  void insert(const std::string& key, const std::string& value) {
    config_[key] = value;
  }

  void insert(const proto::api::Config& config) {
    for (auto kv : config.kvs()) {
      config_[kv.key()] = kv.value();
    }
  }

  void clear() {
    config_.clear();
  }

  void dump(proto::api::Config* config) {
    for (auto& kv : config_) {
      auto keyValue = config->add_kvs();
      keyValue->set_key(kv.first);
      keyValue->set_value(kv.second);
    }
  }

 private:
  std::map<std::string, std::string> config_;
  static const std::set<std::string> apiVars_;
};

}  // namespace config
}  // namespace api
}  // namespace heron

#endif  // HERON_API_CONFIG_CONFIG_H_
