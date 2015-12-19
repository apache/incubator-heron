#include <functional>
#include <iostream>

#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "grouping/grouping.h"
#include "grouping/lowest-grouping.h"

namespace heron {
namespace stmgr {

LowestGrouping::LowestGrouping(const std::vector<sp_int32>& _task_ids)
    : Grouping(_task_ids) {
  lowest_taskid_ = _task_ids[0];
  for (sp_uint32 i = 1; i < _task_ids.size(); ++i) {
    if (lowest_taskid_ > _task_ids[i]) {
      lowest_taskid_ = _task_ids[i];
    }
  }
}

LowestGrouping::~LowestGrouping() {}

void LowestGrouping::GetListToSend(const proto::system::HeronDataTuple&,
                                   std::list<sp_int32>& _return) {
  _return.push_back(lowest_taskid_);
}

}  // namespace stmgr
}  // namespace heron
