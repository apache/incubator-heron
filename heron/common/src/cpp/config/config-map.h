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

#include "config/parameters.h"

namespace heron {
namespace config {

/**
 * Config is a container for key, value map 
 */
class Config : public Parameters {
 public:
  Config() { }
  virtual ~Config() { }

  Config(const Config& _config) : Parameters(_config) {
  }

  Config& operator = (const Config& _config) {
    Parameters::operator = (_config);
    return *this;
  }

  class Builder : public Parameters::Builder {
   public:
    Builder() { }

    Builder(const Builder& _builder) : Parameters::Builder(_builder) {
    }

    Builder& operator = (const Builder& _builder) {
      Parameters::Builder::operator = (_builder);
      return *this;
    }

    Config build() {
      return Config(*this);
    }
  };

  explicit Config(const Builder& builder) : Parameters(builder) {
  }
};

}  // namespace config
}  // namespace heron

#endif
