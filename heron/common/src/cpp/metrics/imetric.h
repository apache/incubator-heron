/*
 * Copyright 2015 Twitter, Inc.
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

//////////////////////////////////////////////////////////////////////////////
//
// imetric.h
//
// Duplication of IMetric.java
//////////////////////////////////////////////////////////////////////////////
#ifndef __IMETRIC_H_
#define __IMETRIC_H_

#include "basics/basics.h"
#include "proto/messages.h"

namespace heron {
namespace common {

class IMetric {
 public:
  virtual ~IMetric() {}
  virtual void GetAndReset(const sp_string& _prefix,
                           proto::system::MetricPublisherPublishMessage* _message) = 0;
};
}  // namespace common
}  // namespace heron

#endif
