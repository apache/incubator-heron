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

#ifndef HERON_API_SPOUT_BASE_RICH_SPOUT_H_
#define HERON_API_SPOUT_BASE_RICH_SPOUT_H_

#include <string>

#include "spout/irich-spout.h"
#include "config/config.h"

namespace heron {
namespace api {
namespace spout {

class BaseRichSpout : public IRichSpout {
 public:
  virtual void close() { }
  virtual void activate() { }
  virtual void deactivate() { }
  virtual void ack(int64_t) { }
  virtual void fail(int64_t msgId) { }
  virtual std::shared_ptr<config::Config> getComponentConfiguration() {
    return nullptr;
  }
};

}  // namespace spout
}  // namespace api
}  // namespace heron

#endif  // HERON_API_SPOUT_BASE_RICH_SPOUT_H_
