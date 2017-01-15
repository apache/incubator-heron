/*
 * Copyright 2015 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

////////////////////////////////////////////////////////////////
//
// topology_config_defaults.h
//
// This file deals with default values for topology config
// variables. It takes in a config file to load the defaults
//
///////////////////////////////////////////////////////////////
#ifndef TOPOLOGY_CONFIG_READER_H_
#define TOPOLOGY_CONFIG_READER_H_

#include "config/yaml-file-reader.h"
#include "proto/messages.h"

namespace heron {
namespace config {

class TopologyConfigReader : public YamlFileReader {
 public:
  TopologyConfigReader(EventLoop* eventLoop, const sp_string& _defaults_file);
  virtual ~TopologyConfigReader();

  // Fill topology config with default values in case
  // things are not specified
  void BackFillTopologyConfig(proto::api::Topology* _topology);

  virtual void OnConfigFileLoad();
};
}  // namespace config
}  // namespace heron

#endif
