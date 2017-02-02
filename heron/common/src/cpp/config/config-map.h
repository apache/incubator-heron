// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#ifndef CONFIG_MAP_H
#define CONFIG_MAP_H

#include <string>
#include <vector>

#include "basics/basics.h"
#include "config/common-config-vars.h"

namespace heron {
namespace config {

/**
 * Config is a container for key, value map 
 */
class Config {
 public:
  Config() { }
  virtual ~Config() { }

  Config(const Config& _config) : params_(_config.params_) {
  }

  Config& operator = (const Config& _config) {
    params_ = _config.params_;
    return *this;
  }

  int size() {
    return params_.size();
  }

  // get the value associated with the key as string
  std::string getstr(const std::string& key) const {
    return params_.get(key);
  }

  std::string getstr(const std::string& key, const std::string& defaults) {
    return params_.getstr(key, defaults);
  }

  // get the value associated with the key as bool
  bool getbool(const std::string& key) {
    return params_.getbool(key);
  }

  bool getbool(const std::string& key, bool defaults) {
    return params_.getbool(key, defaults);
  }

  // get the value associated with the key as long
  int64_t getint64(const std::string& key) {
    return params_.getint64(key);
  }

  int64_t getint64(const std::string& key, int64_t defaults) {
    return params_.getint64(key, defaults);
  }

  // get the value associated with the key as integer
  int32_t getint32(const std::string& key) {
    return params_.getint32(key);
  }

  int32_t getint32(const std::string& key, int32_t defaults) {
    return params_.getint32(key, defaults);
  }

  // get the value associated with the key as double
  double getdouble(const std::string& key) {
    return params_.getdouble(key);
  }

  double getdouble(const std::string& key, double defaults) {
    return params_.getdouble(key, defaults);
  }

  // check if the key exists or not
  bool contains(const std::string& key) {
    return params_.contains(key);
  }

  class Builder {
   public:
    Builder() { }

    Builder(const Builder& _builder) : builder_(_builder.builder_) {
    }

    Builder& operator = (const Builder& _builder) {
      builder_ = _builder.builder_;
      return *this;
    }

    // put a string value associated with the key
    Builder& putstr(const std::string& key, const std::string& value) {
      builder_.putstr(key, value);
      return *this;
    }

    // put a bool value associated with the key
    Builder& putbool(const std::string& key, bool value) {
      builder_.putbool(key, value);
      return *this;
    }

    // put a long value associated with the key
    Builder& putint64(const std::string& key, int64_t value) {
      builder_.putint64(key, value);
      return *this;
    }

    // put an int value associated with the key
    Builder& putint32(const std::string& key, int32_t value) {
      builder_.putint32(key, value);
      return *this;
    }

    // put a double value associated with the key
    Builder& putdouble(const std::string& key, double value) {
      builder_.putdouble(key, value);
      return *this;
    }

    // put a ptr value associated with the key
    Builder& putptr(const std::string& key, void* value) {
      builder_.putptr(key, value);
      return *this;
    }

    Builder& putall(const Builder& _builder) {
      builder_.putall(_builder.builder_);
      return *this;
    }

    Config build() {
      return Config(*this);
    }

   private:
    heron::common::Parameters::Builder builder_;
    friend class Config;
  };

  // given a builder return a config structure
  explicit Config(const Builder& _builder) {
    params_ = heron::common::Parameters(_builder.builder_);
  }

  // iteratively expand the environment variables and the config
  Config expand();

 private:
  std::string combine(const std::vector<std::string>& paths);
  std::string substitute(const std::string& _value);
  heron::common::Parameters  params_;
};

}  // namespace config
}  // namespace heron

#endif
