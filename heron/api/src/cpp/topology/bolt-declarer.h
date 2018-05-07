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

#ifndef HERON_API_TOPOLOGY_BOLT_DECLARER_H_
#define HERON_API_TOPOLOGY_BOLT_DECLARER_H_

#include <list>
#include <string>
#include <memory>

#include "proto/messages.h"

#include "config/config.h"
#include "bolt/irich-bolt.h"
#include "topology/output-fields-getter.h"
#include "topology/base-component-declarer.h"
#include "tuple/fields.h"

namespace heron {
namespace api {
namespace topology {

class BoltDeclarer : public BaseComponentDeclarer<BoltDeclarer> {
 public:
  BoltDeclarer(const std::string& boltName,
               const std::string& boltConstructorFunction,
               std::shared_ptr<bolt::IRichBolt> bolt, int taskParallelism)
    : BaseComponentDeclarer<BoltDeclarer>(boltName, boltConstructorFunction, bolt,
                                          taskParallelism) {
    output_.reset(new OutputFieldsGetter());
    bolt->declareOutputFields(output_);
  }

  virtual ~BoltDeclarer() {
    for (auto sr : inputs_) {
      delete sr;
    }
  }

  virtual BoltDeclarer* returnThis() {
    return this;
  }

  void dump(proto::api::Topology* topology) {
    proto::api::Bolt* bolt = topology->add_bolts();
    BaseComponentDeclarer<BoltDeclarer>::dump(bolt->mutable_comp());
    for (auto sr : inputs_) {
      bolt->add_inputs()->CopyFrom(*sr);
    }
    output_->dump(bolt);
  }

  BoltDeclarer* fieldsGrouping(const std::string& componentName, const tuple::Fields& fields) {
    return fieldsGrouping(componentName, utils::Utils::DEFAULT_STREAM_ID, fields);
  }

  BoltDeclarer* fieldsGrouping(const std::string& componentName,
                               const std::string& streamName,
                               const tuple::Fields& fields) {
    auto inputStream = new proto::api::InputStream();
    inputStream->mutable_stream()->set_id(streamName);
    inputStream->mutable_stream()->set_component_name(componentName);
    inputStream->set_gtype(proto::api::Grouping::FIELDS);
    auto schema = inputStream->mutable_grouping_fields();
    for (auto fieldName : fields.getAllFields()) {
      auto key = schema->add_keys();
      key->set_key(fieldName);
      key->set_type(proto::api::Type::OBJECT);
    }
    return add(inputStream);
  }

  BoltDeclarer* globalGrouping(const std::string& componentName) {
    return globalGrouping(componentName, utils::Utils::DEFAULT_STREAM_ID);
  }

  BoltDeclarer* globalGrouping(const std::string& componentName,
                               const std::string& streamName) {
    auto inputStream = new proto::api::InputStream();
    inputStream->mutable_stream()->set_id(streamName);
    inputStream->mutable_stream()->set_component_name(componentName);
    inputStream->set_gtype(proto::api::Grouping::LOWEST);
    return add(inputStream);
  }

  BoltDeclarer* shuffleGrouping(const std::string& componentName) {
    return shuffleGrouping(componentName, utils::Utils::DEFAULT_STREAM_ID);
  }

  BoltDeclarer* shuffleGrouping(const std::string& componentName,
                                const std::string& streamName) {
    auto inputStream = new proto::api::InputStream();
    inputStream->mutable_stream()->set_id(streamName);
    inputStream->mutable_stream()->set_component_name(componentName);
    inputStream->set_gtype(proto::api::Grouping::SHUFFLE);
    return add(inputStream);
  }

  BoltDeclarer* noneGrouping(const std::string& componentName) {
    return noneGrouping(componentName, utils::Utils::DEFAULT_STREAM_ID);
  }

  BoltDeclarer* noneGrouping(const std::string& componentName,
                             const std::string& streamName) {
    auto inputStream = new proto::api::InputStream();
    inputStream->mutable_stream()->set_id(streamName);
    inputStream->mutable_stream()->set_component_name(componentName);
    inputStream->set_gtype(proto::api::Grouping::NONE);
    return add(inputStream);
  }

  BoltDeclarer* allGrouping(const std::string& componentName) {
    return allGrouping(componentName, utils::Utils::DEFAULT_STREAM_ID);
  }

  BoltDeclarer* allGrouping(const std::string& componentName,
                            const std::string& streamName) {
    auto inputStream = new proto::api::InputStream();
    inputStream->mutable_stream()->set_id(streamName);
    inputStream->mutable_stream()->set_component_name(componentName);
    inputStream->set_gtype(proto::api::Grouping::ALL);
    return add(inputStream);
  }

  BoltDeclarer* directGrouping(const std::string& componentName) {
    return directGrouping(componentName, utils::Utils::DEFAULT_STREAM_ID);
  }

  BoltDeclarer* directGrouping(const std::string& componentName,
                                      const std::string& streamName) {
    throw std::runtime_error("direct Grouping not implemented");
  }

  BoltDeclarer* customGrouping(const std::string& componentName) {
    return customGrouping(componentName, utils::Utils::DEFAULT_STREAM_ID);
  }

  BoltDeclarer* customGrouping(const std::string& componentName,
                                      const std::string& streamName) {
    throw std::runtime_error("custom Grouping not implemented");
  }

 private:
  BoltDeclarer* add(proto::api::InputStream* input) {
    inputs_.push_back(input);
    return this;
  }

  std::shared_ptr<OutputFieldsGetter> output_;
  std::list<proto::api::InputStream*> inputs_;
};

}  // namespace topology
}  // namespace api
}  // namespace heron

#endif  // HERON_API_TOPOLOGY_BOLT_DECLARER_H_
