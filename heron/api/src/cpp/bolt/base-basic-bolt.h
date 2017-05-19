/*
 * Copyright 2017 Twitter, Inc.
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

#ifndef HERON_API_BOLT_BASE_BASIC_BOLT_H_
#define HERON_API_BOLT_BASE_BASIC_BOLT_H_

#include <string>
#include <memory>

#include "config/config.h"
#include "bolt/ibasic-bolt.h"

namespace heron {
namespace api {
namespace bolt {

class BaseBasicBolt : public IBasicBolt {
 public:
  virtual void prepare(std::shared_ptr<config::Config> conf,
                       std::shared_ptr<topology::TaskContext> context) {
  }

  virtual void cleanup() {
  }

  virtual std::shared_ptr<config::Config> getComponentConfiguration() {
    return NULL;
  }
};

}  // namespace bolt
}  // namespace api
}  // namespace heron

#endif  // HERON_API_BOLT_BASE_BASIC_BOLT_H_
