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

#ifndef HERON_API_SERIALIZER_TUPLE_SERIALIZER_HELPER_H_
#define HERON_API_SERIALIZER_TUPLE_SERIALIZER_HELPER_H_

#include <istream>
#include <ostream>
#include <string>
#include <tuple>
#include <vector>

#include "exceptions/serialization-exception.h"
#include "serializer/ipluggable-serializer.h"
#include "serializer/cereal-serializer.h"
#include "serializer/string-serializer.h"

namespace heron {
namespace api {
namespace serializer {

template<typename T>
void IPluggableSerializer::serialize(std::ostream& ss, const T& t) {
  if (cereal_serializer_) {
    cereal_serializer_->serialize(ss, t);
  } else if (string_serializer_) {
    string_serializer_->serialize(ss, t);
  } else {
    std::cout << "Unknown serializer type";
    ::exit(1);
  }
}

template<typename T>
void IPluggableSerializer::deserialize(std::istream& ss, T& t) {
  if (cereal_serializer_) {
    cereal_serializer_->deserialize(ss, t);
  } else if (string_serializer_) {
    string_serializer_->deserialize(ss, t);
  } else {
    std::cout << "Unknown serializer type";
    ::exit(1);
  }
}

// ------------- UTILITY---------------
template<int...> struct index_tuple{};

template<int I, typename IndexTuple, typename... Types>
struct make_indexes_impl;

template<int I, int... Indexes, typename T, typename ... Types>
struct make_indexes_impl<I, index_tuple<Indexes...>, T, Types...> {
    typedef typename make_indexes_impl<I + 1, index_tuple<Indexes..., I>, Types...>::type type;
};

template<int I, int... Indexes>
struct make_indexes_impl<I, index_tuple<Indexes...> > {
    typedef index_tuple<Indexes...> type;
};

template<typename ... Types>
struct make_indexes : make_indexes_impl<0, index_tuple<>, Types...> {};

template<typename T>
int sizeOfTuple(const T& t) {
  return std::tuple_size<T>::value;
}

class TupleSerializerHelper {
 private:
  template<typename Last>
  static void serialize_each(std::vector<std::string>& retval,
                             std::shared_ptr<IPluggableSerializer> serializer,
                             Last&& last) {
    std::ostringstream ostr;
    serializer->serialize(ostr, last);
    retval.push_back(ostr.str());
  }

  template<typename First, typename ... Rest>
  static void serialize_each(std::vector<std::string>& retval,
                             std::shared_ptr<IPluggableSerializer> serializer,
                             First&& first, Rest&&...rest) {  // NOLINT (build/c++11)
    std::ostringstream ostr;
    serializer->serialize(ostr, first);
    retval.push_back(ostr.str());
    serialize_each(retval, serializer, rest...);
  }

  template<int ... Indexes, typename ... Args>
  static void serializer_helper(std::vector<std::string>& retval,
                                std::shared_ptr<IPluggableSerializer> serializer,
                                index_tuple<Indexes...>,
                                std::tuple<Args...>&& tup) {  // NOLINT (build/c++11)
    serialize_each(retval, serializer, std::forward<Args>(std::get<Indexes>(tup))...);
  }

  template<typename Last>
  static void deserialize_each(std::function<const std::string&(int)> getter,
                               std::shared_ptr<IPluggableSerializer> serializer,
                               int index,
                               Last&& last) {
    std::istringstream istr(getter(index));
    serializer->deserialize(istr, last);
  }

  template<typename First, typename ... Rest>
  static void deserialize_each(std::function<const std::string&(int)> getter,
                               std::shared_ptr<IPluggableSerializer> serializer,
                               int index,
                               First&& first, Rest&&...rest) {  // NOLINT (build/c++11)
    std::istringstream istr(getter(index));
    serializer->deserialize(istr, first);
    deserialize_each(std::move(getter), serializer, index + 1, rest...);
  }

  template<int ... Indexes, typename ... Args>
  static void deserializer_helper(std::function<const std::string&(int)> getter,
                                  std::shared_ptr<IPluggableSerializer> serializer,
                                  index_tuple<Indexes...>,
                                  std::tuple<Args...>&& tup) {  // NOLINT (build/c++11)
    deserialize_each(std::move(getter), serializer, 0,
                     std::forward<Args>(std::get<Indexes>(tup))...);
  }

 public:
  template<typename ... Args>
  static void serialize_tuple(std::vector<std::string>& retval,
                              std::shared_ptr<IPluggableSerializer> serializer,
                              std::tuple<Args...>& tup) {
    serializer_helper(retval, serializer,
                      typename make_indexes<Args...>::type(),
                      std::forward<std::tuple<Args...>>(tup));
  }

  template<typename ... Args>
  static void deserialize_tuple(std::function<const std::string&(int)> getter,
                                int nStrings,
                                std::shared_ptr<IPluggableSerializer> serializer,
                                std::tuple<Args...>& tup) {
    if (sizeOfTuple(tup) != nStrings) {
      throw exceptions::SerializerException("Cardinality mismatch while deserialization");
    }
    deserializer_helper(std::move(getter), serializer,
                        typename make_indexes<Args...>::type(),
                        std::forward<std::tuple<Args...>>(tup));
  }
};

}  // namespace serializer
}  // namespace api
}  // namespace heron

#endif  // HERON_API_SERIALIZER_TUPLE_SERIALIZER_HELPER_H_
