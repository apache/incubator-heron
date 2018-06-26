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

#ifndef HERON_API_TUPLE_FIELDS_H_
#define HERON_API_TUPLE_FIELDS_H_

#include <string>
#include <list>
#include <initializer_list>
#include <set>

namespace heron {
namespace api {
namespace tuple {

class Fields {
 public:
  explicit Fields(std::initializer_list<std::string> fields)
    : fields_(fields) {
    if (!checkFields()) {
      throw std::invalid_argument("Duplicate field names");
    }
  }

  explicit Fields(const std::list<std::string>& fields)
    : fields_(fields) {
    if (!checkFields()) {
      throw std::invalid_argument("Duplicate field names");
    }
  }

  /**
   * Returns the fields
   */
  const std::list<std::string>& getAllFields() const {
    return fields_;
  }

  /**
   * The cardinality of the fields
   */
  int getCardinality() {
    return fields_.size();
  }

  /**
   * Get the nth field
   */
  const std::string& getField(int index) {
    if (getCardinality() < index) {
      throw std::invalid_argument("Field cardinality too small");
    }
    std::list<std::string>::const_iterator iter = fields_.begin();
    for (auto i = 0; i < index; ++i) {
      ++iter;
    }
    return *iter;
  }

  /**
   * Get the index of a particular field
   */
  int fieldIndex(const std::string& field) {
    int index = 0;
    for (auto fieldName : fields_) {
      if (fieldName == field) {
        return index;
      } else {
        ++index;
      }
    }
    throw std::invalid_argument("Field does not exist");
  }

  /**
   * Return true if field is a fieldName
   */
  bool containsField(const std::string& field) {
    for (auto fieldName : fields_) {
      if (fieldName == field) {
        return true;
      }
    }
    return false;
  }

 private:
  /**
   * Checks the field names to make sure that there is no duplication
   */
  bool checkFields() {
    std::set<std::string> fieldNames;
    for (auto field : fields_) {
      if (fieldNames.find(field) == fieldNames.end()) {
        fieldNames.insert(field);
      } else {
        return false;
      }
    }
    return true;
  }

  // The actual field names
  std::list<std::string> fields_;
};

}  // namespace tuple
}  // namespace api
}  // namespace heron

#endif  // HERON_API_TUPLE_FIELDS_H_
