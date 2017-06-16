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

#include "config/heron-internals-config-reader.h"
#include <string>
#include "config/heron-internals-config-vars.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "proto/messages.h"
#include "network/network.h"
#include "threads/threads.h"

namespace heron {
namespace config {

// Global initialization to facilitate singleton design pattern
OverrideConfigReader* OverrideConfigReader::override_config_reader_ = 0;

OverrideConfigReader::OverrideConfigReader(EventLoop* eventLoop,
                                                       const sp_string& _defaults_file)
    : YamlFileReader(eventLoop, _defaults_file) {
  LoadConfig();
}

OverrideConfigReader::~OverrideConfigReader() { delete override_config_reader_; }

OverrideConfigReader* OverrideConfigReader::Instance() {
  if (override_config_reader_ == 0) {
    LOG(FATAL) << "Singleton OverrideConfigReader has not been created";
  }

  return override_config_reader_;
}

bool OverrideConfigReader::Exists() {
  return (override_config_reader_ != NULL);  // Return true/false
}

void OverrideConfigReader::Create(EventLoop* eventLoop, const sp_string& _defaults_file) {
  if (override_config_reader_) {
    LOG(FATAL) << "Singleton OverrideConfigReader has already been created";
  } else {
    override_config_reader_ = new OverrideConfigReader(eventLoop, _defaults_file);
  }
}

void OverrideConfigReader::Create(const sp_string& _defaults_file) {
  Create(NULL, _defaults_file);
}

void OverrideConfigReader::OnConfigFileLoad() {
  // Nothing really
}

sp_int32 OverrideConfigReader::GetHeronAutoHealWindow() {
  return config_["heron.config.auto_heal_window"].as<int>();
}

sp_int32 OverrideConfigReader::GetHeronAutoHealInterval() {
  return config_["heron.config.auto_heal_interval"].as<int>();
}

}  // namespace config
}  // namespace heron
