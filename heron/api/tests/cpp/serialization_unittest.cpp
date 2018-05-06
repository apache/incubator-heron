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
#include <tuple>
#include <vector>
#include "basics/basics.h"
#include "gtest/gtest.h"

#include "serializer/cereal-serializer.h"
#include "serializer/string-serializer.h"
#include "serializer/tuple-serializer-utils.h"

const std::string& getter(const std::vector<std::string>& vec, int index) {
  return vec[index];
}

// Test the string serialization
TEST(SerialzationTest, string_serialization) {
  std::shared_ptr<heron::api::serializer::IPluggableSerializer> ser(
    new heron::api::serializer::StringSerializer());
  std::vector<std::string> serialized_values;
  std::tuple<int, std::string> input = std::make_tuple(10, "Hello");
  heron::api::serializer::TupleSerializerHelper::serialize_tuple(serialized_values, ser, input);
  std::tuple<int, std::string> output = std::make_tuple(0, "");
  heron::api::serializer::TupleSerializerHelper::deserialize_tuple(std::bind(getter,
                                                                             serialized_values,
                                                                             std::placeholders::_1),
                                                                   serialized_values.size(),
                                                                   ser, output);
  EXPECT_EQ(std::get<0>(input), std::get<0>(output));
  EXPECT_EQ(std::get<1>(input), std::get<1>(output));
}

// Test the cereal serialization
TEST(SerialzationTest, cereal_serialization) {
  std::shared_ptr<heron::api::serializer::IPluggableSerializer> ser(
    new heron::api::serializer::CerealSerializer());
  std::vector<std::string> serialized_values;
  std::tuple<int, std::string> input = std::make_tuple(10, "Hello");
  heron::api::serializer::TupleSerializerHelper::serialize_tuple(serialized_values, ser, input);
  std::tuple<int, std::string> output = std::make_tuple(0, "");
  heron::api::serializer::TupleSerializerHelper::deserialize_tuple(std::bind(getter,
                                                                             serialized_values,
                                                                             std::placeholders::_1),
                                                                   serialized_values.size(),
                                                                   ser, output);
  EXPECT_EQ(std::get<0>(input), std::get<0>(output));
  EXPECT_EQ(std::get<1>(input), std::get<1>(output));
}

int main(int argc, char** argv) {
  heron::common::Initialize(argv[0]);
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
