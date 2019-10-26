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

#include <string>
#include <vector>
#include "basics/basics.h"
#include "config/topology-config-vars.h"
#include "config/topology-config-helper.h"
#include "gtest/gtest.h"
#include "proto/messages.h"

const sp_string SPOUT_NAME = "test_spout";
const sp_string BOLT_NAME = "test_bolt";
const sp_string STREAM_NAME = "stream";
const sp_string MESSAGE_TIMEOUT = "30";  // seconds

int NUM_SPOUT_INSTANCES = 2;
int NUM_BOLT_INSTANCES = 3;

const sp_string TOPOLOGY_USER_CONFIG = "topology.user.test_config:runtime";
const sp_string TOPOLOGY_USER_CONFIG_VALUE = "-1";
const sp_string NEW_TOPOLOGY_USER_CONFIG_VALUE = "1";
const sp_string NEW_TOPOLOGY_USER_CONFIG_VALUE_2 = "11";
const sp_string SPOUT_USER_CONFIG = "topology.user.spout.test_config:runtime";
const sp_string SPOUT_USER_CONFIG_VALUE = "-2";
const sp_string NEW_SPOUT_USER_CONFIG_VALUE = "2";
const sp_string NEW_SPOUT_USER_CONFIG_VALUE_2 = "22";
const sp_string BOLT_USER_CONFIG = "topology.user.bolt.test_config:runtime";
const sp_string BOLT_USER_CONFIG_VALUE = "-3";
const sp_string NEW_BOLT_USER_CONFIG_VALUE = "3";
const sp_string NEW_BOLT_USER_CONFIG_VALUE_2 = "33";

static heron::proto::api::Topology* GenerateDummyTopology(
    const std::string& topology_name, const std::string& topology_id, int num_spouts,
    int num_spout_instances, int num_bolts, int num_bolt_instances,
    const heron::proto::api::Grouping& grouping) {
  heron::proto::api::Topology* topology = new heron::proto::api::Topology();
  topology->set_id(topology_id);
  topology->set_name(topology_name);
  size_t spouts_size = num_spouts;
  size_t bolts_size = num_bolts;
  // Set spouts
  for (size_t i = 0; i < spouts_size; ++i) {
    heron::proto::api::Spout* spout = topology->add_spouts();
    // Set the component information
    heron::proto::api::Component* component = spout->mutable_comp();
    std::string compname = SPOUT_NAME;
    compname += std::to_string(i);
    component->set_name(compname);
    heron::proto::api::ComponentObjectSpec compspec = heron::proto::api::JAVA_CLASS_NAME;
    component->set_spec(compspec);
    // Set the stream information
    heron::proto::api::OutputStream* ostream = spout->add_outputs();
    heron::proto::api::StreamId* tstream = ostream->mutable_stream();
    std::string streamid = STREAM_NAME;
    streamid += std::to_string(i);
    tstream->set_id(streamid);
    tstream->set_component_name(compname);
    heron::proto::api::StreamSchema* schema = ostream->mutable_schema();
    heron::proto::api::StreamSchema::KeyType* key_type = schema->add_keys();
    key_type->set_key("dummy");
    key_type->set_type(heron::proto::api::OBJECT);
    // Set the config
    heron::proto::api::Config* config = component->mutable_config();
    heron::proto::api::Config::KeyValue* kv = config->add_kvs();
    kv->set_key(heron::config::TopologyConfigVars::TOPOLOGY_COMPONENT_PARALLELISM);
    kv->set_value(std::to_string(num_spout_instances));
    // Add user config
    heron::proto::api::Config::KeyValue* kv1 = config->add_kvs();
    kv1->set_key(SPOUT_USER_CONFIG);
    kv1->set_value(SPOUT_USER_CONFIG_VALUE);
  }
  // Set bolts
  for (size_t i = 0; i < bolts_size; ++i) {
    heron::proto::api::Bolt* bolt = topology->add_bolts();
    // Set the component information
    heron::proto::api::Component* component = bolt->mutable_comp();
    std::string compname = BOLT_NAME;
    compname += std::to_string(i);
    component->set_name(compname);
    heron::proto::api::ComponentObjectSpec compspec = heron::proto::api::JAVA_CLASS_NAME;
    component->set_spec(compspec);
    // Set the stream information
    heron::proto::api::InputStream* istream = bolt->add_inputs();
    heron::proto::api::StreamId* tstream = istream->mutable_stream();
    std::string streamid = STREAM_NAME;
    streamid += std::to_string(i);
    tstream->set_id(streamid);
    std::string input_compname = SPOUT_NAME;
    input_compname += std::to_string(i);
    tstream->set_component_name(input_compname);
    istream->set_gtype(grouping);
    // Set the config
    heron::proto::api::Config* config = component->mutable_config();
    heron::proto::api::Config::KeyValue* kv = config->add_kvs();
    kv->set_key(heron::config::TopologyConfigVars::TOPOLOGY_COMPONENT_PARALLELISM);
    kv->set_value(std::to_string(num_bolt_instances));
    // Add user config
    heron::proto::api::Config::KeyValue* kv1 = config->add_kvs();
    kv1->set_key(BOLT_USER_CONFIG);
    kv1->set_value(BOLT_USER_CONFIG_VALUE);
  }
  // Set topology config: message timeout
  heron::proto::api::Config* topology_config = topology->mutable_topology_config();
  heron::proto::api::Config::KeyValue* kv = topology_config->add_kvs();
  kv->set_key(heron::config::TopologyConfigVars::TOPOLOGY_MESSAGE_TIMEOUT_SECS);
  kv->set_value(MESSAGE_TIMEOUT);
  // Add user config
  heron::proto::api::Config::KeyValue* kv1 = topology_config->add_kvs();
  kv1->set_key(TOPOLOGY_USER_CONFIG);
  kv1->set_value(TOPOLOGY_USER_CONFIG_VALUE);

  // Set state
  topology->set_state(heron::proto::api::RUNNING);

  return topology;
}

TEST(TopologyConfigHelper, GetAndSetTopologyConfig) {
  heron::proto::api::Topology* test_topology = GenerateDummyTopology(
      "test_topology", "123", 3, NUM_SPOUT_INSTANCES, 3, NUM_BOLT_INSTANCES,
      heron::proto::api::SHUFFLE);

  // Test initial config
  std::map<std::string, std::string> old_config;
  heron::config::TopologyConfigHelper::GetTopologyRuntimeConfig(*test_topology, old_config);
  EXPECT_EQ(old_config[TOPOLOGY_USER_CONFIG], TOPOLOGY_USER_CONFIG_VALUE);

  // Test GetComponentConfigValue function
  EXPECT_EQ(
      heron::config::TopologyConfigHelper::GetTopologyConfigValue(
          *test_topology, TOPOLOGY_USER_CONFIG, ""),
      TOPOLOGY_USER_CONFIG_VALUE);
  EXPECT_EQ(
      heron::config::TopologyConfigHelper::GetTopologyConfigValue(
          *test_topology, TOPOLOGY_USER_CONFIG + ".bad", ""),
      "");

  // Set and then test updated config
  std::map<std::string, std::string> update;
  update[TOPOLOGY_USER_CONFIG] = NEW_TOPOLOGY_USER_CONFIG_VALUE;
  heron::config::TopologyConfigHelper::SetTopologyRuntimeConfig(test_topology, update);

  std::map<std::string, std::string> updated_config;
  heron::config::TopologyConfigHelper::GetTopologyRuntimeConfig(*test_topology, updated_config);
  EXPECT_EQ(updated_config[TOPOLOGY_USER_CONFIG], NEW_TOPOLOGY_USER_CONFIG_VALUE);

  update[TOPOLOGY_USER_CONFIG] = NEW_TOPOLOGY_USER_CONFIG_VALUE_2;
  heron::config::TopologyConfigHelper::SetTopologyRuntimeConfig(test_topology, update);
  updated_config.clear();
  heron::config::TopologyConfigHelper::GetTopologyRuntimeConfig(*test_topology, updated_config);
  EXPECT_EQ(updated_config[TOPOLOGY_USER_CONFIG], NEW_TOPOLOGY_USER_CONFIG_VALUE_2);
}

TEST(TopologyConfigHelper, GetAndSetComponentConfig) {
  heron::proto::api::Topology* test_topology = GenerateDummyTopology(
      "test_topology", "123", 3, NUM_SPOUT_INSTANCES, 3, NUM_BOLT_INSTANCES,
      heron::proto::api::SHUFFLE);

  std::string test_spout = "test_spout1";
  std::string non_test_spout = "test_spout2";
  std::string test_bolt = "test_bolt2";
  std::string non_test_bolt = "test_bolt1";
  // Test initial config
  std::map<std::string, std::string> old_config;
  heron::config::TopologyConfigHelper::GetComponentRuntimeConfig(*test_topology, test_spout,
      old_config);
  EXPECT_EQ(old_config[SPOUT_USER_CONFIG], SPOUT_USER_CONFIG_VALUE);
  // parallelism is not a runtime config, hence it is not extracted
  EXPECT_EQ(old_config[heron::config::TopologyConfigVars::TOPOLOGY_COMPONENT_PARALLELISM], "");
  old_config.clear();
  heron::config::TopologyConfigHelper::GetComponentRuntimeConfig(*test_topology, test_bolt,
      old_config);
  EXPECT_EQ(old_config[BOLT_USER_CONFIG], BOLT_USER_CONFIG_VALUE);
  // parallelism is not a runtime config, hence it is not extracted
  EXPECT_EQ(old_config[heron::config::TopologyConfigVars::TOPOLOGY_COMPONENT_PARALLELISM], "");

  // Test GetComponentConfigValue function
  EXPECT_EQ(
      heron::config::TopologyConfigHelper::GetComponentConfigValue(
          *test_topology, test_spout, SPOUT_USER_CONFIG, ""),
      SPOUT_USER_CONFIG_VALUE);
  EXPECT_EQ(
      heron::config::TopologyConfigHelper::GetComponentConfigValue(
          *test_topology, test_spout, SPOUT_USER_CONFIG + ".bad", ""),
      "");

  // Set user configs to new values
  std::map<std::string, std::string> update;
  update[SPOUT_USER_CONFIG] = NEW_SPOUT_USER_CONFIG_VALUE;
  heron::config::TopologyConfigHelper::SetComponentRuntimeConfig(test_topology, test_spout, update);
  update.clear();
  update[BOLT_USER_CONFIG] = NEW_BOLT_USER_CONFIG_VALUE;
  heron::config::TopologyConfigHelper::SetComponentRuntimeConfig(test_topology, test_bolt, update);

  // Test user configs are updated
  std::map<std::string, std::string> updated_config;
  heron::config::TopologyConfigHelper::GetComponentRuntimeConfig(
      *test_topology, test_spout, updated_config);
  EXPECT_EQ(updated_config[SPOUT_USER_CONFIG], NEW_SPOUT_USER_CONFIG_VALUE);
  updated_config.clear();
  heron::config::TopologyConfigHelper::GetComponentRuntimeConfig(
      *test_topology, test_bolt, updated_config);
  EXPECT_EQ(updated_config[BOLT_USER_CONFIG], NEW_BOLT_USER_CONFIG_VALUE);

  // Set to new value 2 and verify
  update.clear();
  update[SPOUT_USER_CONFIG] = NEW_SPOUT_USER_CONFIG_VALUE_2;
  heron::config::TopologyConfigHelper::SetComponentRuntimeConfig(test_topology, test_spout, update);
  update.clear();
  update[BOLT_USER_CONFIG] = NEW_BOLT_USER_CONFIG_VALUE_2;
  heron::config::TopologyConfigHelper::SetComponentRuntimeConfig(test_topology, test_bolt, update);

  // Test user configs are updated
  updated_config.clear();
  heron::config::TopologyConfigHelper::GetComponentRuntimeConfig(
      *test_topology, test_spout, updated_config);
  EXPECT_EQ(updated_config[SPOUT_USER_CONFIG], NEW_SPOUT_USER_CONFIG_VALUE_2);
  updated_config.clear();
  heron::config::TopologyConfigHelper::GetComponentRuntimeConfig(
      *test_topology, test_bolt, updated_config);
  EXPECT_EQ(updated_config[BOLT_USER_CONFIG], NEW_BOLT_USER_CONFIG_VALUE_2);
}

TEST(TopologyConfigHelper, GetRuntimeConfigKey) {
  EXPECT_EQ(
      heron::config::TopologyConfigHelper::GetRuntimeConfigKey("conf.test1"),
      "conf.test1:runtime");
}

TEST(TopologyConfigHelper, ConvertToRuntimeConfigs) {
  std::map<std::string, std::string> original_config;
  original_config["conf.test1"] = "a";
  original_config["conf.test2"] = "b";

  std::map<std::string, std::string> runtime_config;
  heron::config::TopologyConfigHelper::ConvertToRuntimeConfigs(original_config, runtime_config);

  EXPECT_EQ(runtime_config["conf.test1:runtime"], "a");
  EXPECT_EQ(runtime_config["conf.test2:runtime"], "b");
}

int main(int argc, char **argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
