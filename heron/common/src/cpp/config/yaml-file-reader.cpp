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

#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "config/yaml-file-reader.h"

namespace heron {
namespace config {

YamlFileReader::YamlFileReader(std::shared_ptr<EventLoop> eventLoop, const sp_string& _config_file)
    : config_file_(_config_file), last_reload_(-1) {
  if (eventLoop) {
    // If we have specified the EventLoopImpl,
    // We would check for the change of config file continously
    reload_cb_ = [this](EventLoop::Status status) { this->CheckForChange(status); };
    eventLoop->registerTimer(reload_cb_, true, 1000000);
  } else {
    // We would load a static config file
    // And never check for change again
    reload_cb_ = NULL;
    LOG(INFO) << "Static Config file " << config_file_ << " is loading.." << std::endl;
    config_ = YAML::LoadFile(config_file_);
    if (!config_.IsMap()) {
      LOG(FATAL) << "Invalid config file " << config_file_ << std::endl;
    }
  }
}

YamlFileReader::~YamlFileReader() {}

void YamlFileReader::LoadConfig() { CheckForChange(EventLoop::TIMEOUT_EVENT); }

void YamlFileReader::CheckForChange(EventLoop::Status) {
  time_t last_changed = FileUtils::getModifiedTime(config_file_);
  if (last_changed > last_reload_) {
    LOG(INFO) << "Config file " << config_file_ << " changed. reloading..";
    config_ = YAML::LoadFile(config_file_);
    if (config_.Type() == YAML::NodeType::Null) {
      LOG(ERROR) << "Empty config file " << config_file_ << std::endl;
      // Empty file. Stuff with some thing
      config_["__NULL__"] = "__NULL__";
    }
    if (!config_.IsMap()) {
      LOG(FATAL) << "Invalid config file " << config_file_ << std::endl;
    }
    OnConfigFileLoad();
    last_reload_ = last_changed;
  }
}

void YamlFileReader::AddIfMissing(const sp_string& _var, const sp_string& _default) {
  if (!config_[_var]) {
    LOG(INFO) << "Adding missing config " << _var << " and setting its default value to "
              << _default << std::endl;
    config_[_var] = _default;
  }
}
}  // namespace config
}  // namespace heron
