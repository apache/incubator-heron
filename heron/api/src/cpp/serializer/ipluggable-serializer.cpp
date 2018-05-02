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

#include <set>
#include <string>
#include <memory>

#include "config/config.h"
#include "serializer/ipluggable-serializer.h"
#include "serializer/cereal-serializer.h"
#include "serializer/string-serializer.h"

namespace heron {
namespace api {
namespace serializer {

IPluggableSerializer* IPluggableSerializer::createSerializer(
                       std::shared_ptr<config::Config> config) {
  std::string className = "string";
  if (config->hasConfig(config::Config::TOPOLOGY_SERIALIZER_CLASSNAME)) {
    className = config->get(config::Config::TOPOLOGY_SERIALIZER_CLASSNAME);
    if (className != "cereal" && className != "string") {
      throw std::runtime_error("Unknown serialization classname");
    }
  }
  if (className == "cereal") {
    CerealSerializer* cereal = new CerealSerializer();
    return new IPluggableSerializer(cereal);
  } else {
    StringSerializer* str = new StringSerializer();
    return new IPluggableSerializer(str);
  }
}

}  // namespace serializer
}  // namespace api
}  // namespace heron
