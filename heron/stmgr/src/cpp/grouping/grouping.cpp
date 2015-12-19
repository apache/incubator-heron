#include <functional>
#include <iostream>

#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "grouping/grouping.h"
#include "grouping/shuffle-grouping.h"
#include "grouping/fields-grouping.h"
#include "grouping/all-grouping.h"
#include "grouping/lowest-grouping.h"
#include "grouping/custom-grouping.h"

namespace heron {
namespace stmgr {

Grouping::Grouping(const std::vector<sp_int32>& _task_ids) : task_ids_(_task_ids) {}

Grouping::~Grouping() {}

Grouping* Grouping::Create(proto::api::Grouping grouping_,
                           const proto::api::InputStream& _is,
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
      LOG(FATAL) << "Direct grouping not supported";
      return NULL;  // keep compiler happy
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
