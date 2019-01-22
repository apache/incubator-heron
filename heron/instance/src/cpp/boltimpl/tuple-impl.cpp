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

#include "boltimpl/tuple-impl.h"
#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

namespace heron {
namespace instance {

TupleImpl::TupleImpl(std::shared_ptr<api::serializer::IPluggableSerializer> serializer,
                     std::shared_ptr<TaskContextImpl> taskContext,
                     const proto::api::StreamId& stream,
                     std::shared_ptr<const proto::system::HeronDataTuple> tup)
  : api::tuple::Tuple(serializer), taskContext_(taskContext), streamInfo_(stream),
    tuple_(tup) {
  creationTime_ = std::chrono::duration_cast<std::chrono::nanoseconds>(
                                   std::chrono::system_clock::now().time_since_epoch()).count();
}

TupleImpl::~TupleImpl() { }

int TupleImpl::size() const {
  return tuple_->values_size();
}

int TupleImpl::fieldIndex(const std::string& field) const {
  return taskContext_->getComponentOutputFields(streamInfo_.component_name(),
                                                streamInfo_.id()).fieldIndex(field);
}

bool TupleImpl::contains(const std::string& field) const {
  return taskContext_->getComponentOutputFields(streamInfo_.component_name(),
                                                streamInfo_.id()).containsField(field);
}

api::tuple::Fields TupleImpl::getFields() const {
  return taskContext_->getComponentOutputFields(streamInfo_.component_name(),
                                                streamInfo_.id());
}

const std::string& TupleImpl::getSourceComponent() const {
  return streamInfo_.component_name();
}

int TupleImpl::getSourceTask() const {
  LOG(FATAL) << "getSourceTask not yet supported";
  return -1;  // make compiler happy
}

const std::string& TupleImpl::getSourceStreamId() const {
  return streamInfo_.id();
}

const std::string& TupleImpl::getUnserializedValue(int index) const {
  return tuple_->values(index);
}

}  // namespace instance
}  // namespace heron
