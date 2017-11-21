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

#ifndef HERON_API_TOPOLOGY_OUTPUT_FIELDS_DECLARER_H_
#define HERON_API_TOPOLOGY_OUTPUT_FIELDS_DECLARER_H_

#include <string>

#include "tuple/fields.h"

namespace heron {
namespace api {
namespace topology {

class OutputFieldsDeclarer {
 public:
  /**
   * Uses default stream id.
   */
  virtual void declare(const tuple::Fields& fields) = 0;

  virtual void declareStream(const std::string& streamId, const tuple::Fields& fields) = 0;
};

}  // namespace topology
}  // namespace api
}  // namespace heron

#endif  // HERON_API_TOPOLOGY_OUTPUT_FIELDS_DECLARER_H_
