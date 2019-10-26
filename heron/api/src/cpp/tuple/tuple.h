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

#ifndef HERON_API_TUPLE_TUPLE_H_
#define HERON_API_TUPLE_TUPLE_H_

#include <string>
#include <tuple>
#include <vector>

#include "tuple/fields.h"
#include "serializer/tuple-serializer-utils.h"

namespace heron {
namespace api {
namespace tuple {

class Tuple {
 public:
  explicit Tuple(std::shared_ptr<serializer::IPluggableSerializer> serializer)
    : serializer_(serializer) { }

  virtual ~Tuple() { }

  /**
   * Returns the number of fields in this tuple.
   */
  virtual int size() const = 0;

  /**
   * Returns the position of the specified field in this tuple.
   */
  virtual int fieldIndex(const std::string& field) const = 0;

  /**
   * Returns true if this tuple contains the specified name of the field.
   */
  virtual bool contains(const std::string& field) const = 0;

  /**
   * Gets all the values in this tuple.
   */
  template<typename ... Args>
  void getValues(std::tuple<Args...>& tup) {
    serializer::TupleSerializerHelper::deserialize_tuple(std::bind(&Tuple::getUnserializedValue,
                                                         this, std::placeholders::_1),
                                                         size(),
                                                         serializer_, tup);
  }

  /**
   * Gets the names of the fields in this tuple.
   */
  virtual tuple::Fields getFields() const = 0;

  /**
   * Gets the id of the component that created this tuple.
   */
  virtual const std::string& getSourceComponent() const = 0;

  /**
   * Gets the id of the task that created this tuple.
   */
  virtual int getSourceTask() const = 0;

  /**
   * Gets the id of the stream that this tuple was emitted to.
   */
  virtual const std::string& getSourceStreamId() const = 0;

  /**
   * Internal method used by serialization helper
   */
  virtual const std::string& getUnserializedValue(int index) const = 0;

 private:
  std::shared_ptr<serializer::IPluggableSerializer> serializer_;
};

}  // namespace tuple
}  // namespace api
}  // namespace heron

#endif  // HERON_API_TUPLE_TUPLE_H_
