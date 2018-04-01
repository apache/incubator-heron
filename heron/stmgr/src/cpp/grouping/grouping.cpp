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

#include "grouping/grouping.h"
#include <algorithm>
#include <functional>
#include <iostream>
#include <list>
#include <vector>
#include "grouping/direct-grouping.h"
#include "grouping/shuffle-grouping.h"
#include "grouping/fields-grouping.h"
#include "grouping/all-grouping.h"
#include "grouping/lowest-grouping.h"
#include "grouping/custom-grouping.h"
#include "proto/messages.h"
#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

namespace heron {
namespace stmgr {

Grouping::Grouping(const std::vector<sp_int32>& _task_ids)
  : task_ids_(_task_ids) {
  sort(task_ids_.begin(), task_ids_.end());
}

Grouping::~Grouping() {}

Grouping* Grouping::Create(proto::api::Grouping grouping_, const proto::api::InputStream& _is,
                           const proto::api::StreamSchema& _schema,
                           const std::vector<sp_int32>& _task_ids) {
  switch (grouping_) {
    case proto::api::SHUFFLE: {
      return new ShuffleGrouping(_task_ids);
      break;
    }

    case proto::api::FIELDS: {
      return new FieldsGrouping(_is, _schema, _task_ids);
      break;
    }

    case proto::api::ALL: {
      return new AllGrouping(_task_ids);
      break;
    }

    case proto::api::LOWEST: {
      return new LowestGrouping(_task_ids);
      break;
    }

    case proto::api::NONE: {
      // This is what storm does right now
      return new ShuffleGrouping(_task_ids);
      break;
    }

    case proto::api::DIRECT: {
      return new DirectGrouping(_task_ids);
      break;
    }

    case proto::api::CUSTOM: {
      return new CustomGrouping(_task_ids);
      break;
    }

    default: {
      LOG(FATAL) << "Unknown grouping " << grouping_;
      return NULL;  // keep compiler happy
      break;
    }
  }
}

}  // namespace stmgr
}  // namespace heron
