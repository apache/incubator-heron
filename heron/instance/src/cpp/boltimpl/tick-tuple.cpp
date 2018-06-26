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

#include <string>

#include "boltimpl/tick-tuple.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

namespace heron {
namespace instance {

const std::string TickTuple::componentName_ = "__system"; // NOLINT
const std::string TickTuple::streamId_ = "__tick"; // NOLINT

TickTuple::TickTuple(std::shared_ptr<api::serializer::IPluggableSerializer> serializer)
  : api::tuple::Tuple(serializer) {
}

TickTuple::~TickTuple() { }

int TickTuple::size() const {
  return 0;
}

int TickTuple::fieldIndex(const std::string& field) const {
  return -1;
}

bool TickTuple::contains(const std::string& field) const {
  return false;
}

api::tuple::Fields TickTuple::getFields() const {
  return api::tuple::Fields({});
}

const std::string& TickTuple::getSourceComponent() const {
  return componentName_;
}

int TickTuple::getSourceTask() const {
  return -1;
}

const std::string& TickTuple::getSourceStreamId() const {
  return streamId_;
}

const std::string& TickTuple::getUnserializedValue(int index) const {
  return EMPTY_STRING;
}

}  // namespace instance
}  // namespace heron
