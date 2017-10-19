/*
 * Copyright 2017 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HERON_API_BOLT_IRICHBOLT_H_
#define HERON_API_BOLT_IRICHBOLT_H_

#include "bolt/ibolt.h"
#include "topology/icomponent.h"

namespace heron {
namespace api {
namespace bolt {

/**
 * IRichBolt is one of the main interface to use to implement components of the topology.
 */
class IRichBolt : public IBolt, public topology::IComponent {
};

}  // namespace bolt
}  // namespace api
}  // namespace heron

#endif  // HERON_API_BOLT_IRICHBOLT_H_
