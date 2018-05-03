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

#ifndef HERON_INSTANCE_BOLT_TICKTUPLE_H_
#define HERON_INSTANCE_BOLT_TICKTUPLE_H_

#include <string>

#include "proto/messages.h"
#include "network/network.h"
#include "basics/basics.h"

#include "tuple/tuple.h"
#include "tuple/fields.h"
#include "serializer/ipluggable-serializer.h"

namespace heron {
namespace instance {

class TickTuple : public api::tuple::Tuple {
 public:
  explicit TickTuple(std::shared_ptr<api::serializer::IPluggableSerializer> serializer);
  virtual ~TickTuple();

  // All the interfaces

  /**
   * Returns the number of fields in this tuple.
   */
  virtual int size() const;

  /**
   * Returns the position of the specified field in this tuple.
   */
  virtual int fieldIndex(const std::string& field) const;

  /**
   * Returns true if this tuple contains the specified name of the field.
   */
  virtual bool contains(const std::string& field) const;

  /**
   * Gets the names of the fields in this tuple.
   */
  virtual api::tuple::Fields getFields() const;

  /**
   * Gets the id of the component that created this tuple.
   */
  virtual const std::string& getSourceComponent() const;

  /**
   * Gets the id of the task that created this tuple.
   */
  virtual int getSourceTask() const;

  /**
   * Gets the id of the stream that this tuple was emitted to.
   */
  virtual const std::string& getSourceStreamId() const;

  virtual const std::string& getUnserializedValue(int index) const;

 private:
  static const std::string componentName_;
  static const std::string streamId_;
};

}  // namespace instance
}  // namespace heron

#endif  // HERON_INSTANCE_BOLT_TICKTUPLE_H_
