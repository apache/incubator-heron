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

#ifndef HERON_API_TOPOLOGY_OUTPUT_FIELDS_GETTER_H_
#define HERON_API_TOPOLOGY_OUTPUT_FIELDS_GETTER_H_

#include <string>
#include <map>

#include "proto/messages.h"

#include "tuple/fields.h"
#include "topology/output-fields-declarer.h"
#include "utils/utils.h"

namespace heron {
namespace api {
namespace topology {

class OutputFieldsGetter : public OutputFieldsDeclarer {
 public:
  OutputFieldsGetter() { }
  virtual ~OutputFieldsGetter() {
    for (auto& kv : fields_) {
      delete kv.second;
    }
    fields_.clear();
  }
  /**
   * Uses default stream id.
   */
  virtual void declare(const tuple::Fields& fields) {
    declareStream(utils::Utils::DEFAULT_STREAM_ID, fields);
  }

  virtual void declareStream(const std::string& streamId, const tuple::Fields& fields) {
    if (fields_.find(streamId) != fields_.end()) {
      throw std::invalid_argument("The stream " + streamId + " already declared");
    }
    auto schema = new proto::api::StreamSchema();
    for (auto fieldName : fields.getAllFields()) {
      auto key = schema->add_keys();
      key->set_key(fieldName);
      key->set_type(proto::api::Type::OBJECT);
    }
    fields_[streamId] = schema;
  }

  void dump(proto::api::Spout* spout) {
    for (auto& kv : fields_) {
      auto outstream = spout->add_outputs();
      outstream->mutable_stream()->set_id(kv.first);
      outstream->mutable_stream()->set_component_name(spout->comp().name());
      outstream->mutable_schema()->CopyFrom(*kv.second);
    }
  }

  void dump(proto::api::Bolt* bolt) {
    for (auto& kv : fields_) {
      auto outstream = bolt->add_outputs();
      outstream->mutable_stream()->set_id(kv.first);
      outstream->mutable_stream()->set_component_name(bolt->comp().name());
      outstream->mutable_schema()->CopyFrom(*kv.second);
    }
  }

 private:
  std::map<std::string, proto::api::StreamSchema*> fields_;
};

}  // namespace topology
}  // namespace api
}  // namespace heron

#endif  // HERON_API_TOPOLOGY_OUTPUT_FIELDS_GETTER_H_
