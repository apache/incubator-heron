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

#ifndef HERON_API_SERIALIZER_STRING_SERIALIZER_H_
#define HERON_API_SERIALIZER_STRING_SERIALIZER_H_

#include <ostream>
#include <istream>
#include "serializer/ipluggable-serializer.h"

namespace heron {
namespace api {
namespace serializer {

class StringSerializer : public IPluggableSerializer {
 public:
  StringSerializer() : IPluggableSerializer(this) {
  }

  virtual ~StringSerializer() {
  }

  template<typename T>
  void serialize(std::ostream& ss, const T& t) {
    ss << t;
  }

  template<typename T>
  void deserialize(std::istream &ss, T& t) {
    ss >> t;
  }
};

}  // namespace serializer
}  // namespace api
}  // namespace heron

#endif  // HERON_API_SERIALIZER_STRING_SERIALIZER_H_
