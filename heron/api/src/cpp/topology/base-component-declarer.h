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

#ifndef HERON_API_TOPOLOGY_BASE_COMPONENT_DECLARER_H_
#define HERON_API_TOPOLOGY_BASE_COMPONENT_DECLARER_H_

#include <map>
#include <string>
#include <memory>

#include "proto/messages.h"

#include "config/config.h"
#include "topology/icomponent.h"

namespace heron {
namespace api {
namespace topology {

template<typename T>
class BaseComponentDeclarer {
 public:
  BaseComponentDeclarer(const std::string& componentName,
                        const std::string& componentConstructorFunction,
                        std::shared_ptr<IComponent> component, int taskParallelism)
    : componentName_(componentName),
      componentConstructorFunction_(componentConstructorFunction) {
    componentConfiguration_ = component->getComponentConfiguration();
    if (!componentConfiguration_) {
      componentConfiguration_.reset(new config::Config());
    }
    componentConfiguration_->setComponentParallelism(taskParallelism);
  }

  virtual ~BaseComponentDeclarer() { }

  T* addConfiguration(const std::map<std::string, std::string>& config) {
    componentConfiguration_->insert(config);
    return returnThis();
  }

  T* addConfiguration(const std::string& key, const std::string& value) {
    componentConfiguration_->insert(key, value);
    return returnThis();
  }

  T* setDebug(bool debug) {
    componentConfiguration_->setDebug(debug);
    return returnThis();
  }

  virtual T* returnThis() = 0;

 protected:
  void dump(proto::api::Component* component) {
    component->set_name(componentName_);
    component->set_spec(proto::api::ComponentObjectSpec::CPP_CLASS_INFO);
    component->mutable_cpp_class_info()->set_class_constructor(componentConstructorFunction_);
    componentConfiguration_->dump(component->mutable_config());
  }

 private:
  std::string componentName_;
  std::string componentConstructorFunction_;
  std::shared_ptr<config::Config> componentConfiguration_;
};

}  // namespace topology
}  // namespace api
}  // namespace heron

#endif  // HERON_API_TOPOLOGY_BASE_COMPONENT_DECLARER_H_
