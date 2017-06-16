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
// override-config-reader.h
//
// This file deals with default values for override config
// variables. It takes in a config file to load the defaults
// It is a singleton so the whole process could access it
//
///////////////////////////////////////////////////////////////
#ifndef OVERRIDE_CONFIG_READER_H
#define OVERRIDE_CONFIG_READER_H
#include "basics/sptypes.h"
#include "config/yaml-file-reader.h"

class EventLoop;

namespace heron {
namespace config {

class OverrideConfigReader : public YamlFileReader {
 public:
  // Return the singleton if there is one,
  // or NULL if there is not.
  static OverrideConfigReader* Instance();
  // Check whether the singleton is created or not
  static bool Exists();
  // Create a singleton reader from a config file,
  // which will check and reload the config change
  static void Create(EventLoop* eventLoop, const sp_string& _defaults_file);
  // Create a singleton reader from a config file,
  // which will not check or reload the config change
  static void Create(const sp_string& _defaults_file);

  virtual void OnConfigFileLoad();

  /**
  * Auto restart backpressure Config Getters
  **/
  // The time window to determine if a contianer is in backpressure state
  sp_int32 GetHeronAutoHealWindow();

  // The time interval to restart a container in backpressure state
  sp_int32 GetHeronAutoHealInterval();


 protected:
  OverrideConfigReader(EventLoop* eventLoop, const sp_string& _defaults_file);
  virtual ~OverrideConfigReader();

  static OverrideConfigReader* override_config_reader_;
};
}  // namespace config
}  // namespace heron

#endif
