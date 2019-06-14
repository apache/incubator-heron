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

////////////////////////////////////////////////////////////////
//
// yaml-file-reader.h
//
// This file deals with reading yaml config file.
// It also does auto reloads if the file changes
//
///////////////////////////////////////////////////////////////
#ifndef YAML_FILE_READER_H_
#define YAML_FILE_READER_H_

#include "yaml-cpp/yaml.h"
#include "basics/callback.h"
#include "basics/sptypes.h"
#include "network/event_loop.h"

namespace heron {
namespace config {

class YamlFileReader {
 public:
  YamlFileReader(std::shared_ptr<EventLoop> eventLoop, const sp_string& _config_file);
  virtual ~YamlFileReader();

 protected:
  virtual void OnConfigFileLoad() = 0;
  void LoadConfig();
  void AddIfMissing(const sp_string& _var, const sp_string& _default);

  YAML::Node config_;

 private:
  void CheckForChange(EventLoop::Status);

  VCallback<EventLoop::Status> reload_cb_;
  sp_string config_file_;
  time_t last_reload_;
};
}  // namespace config
}  // namespace heron

#endif
