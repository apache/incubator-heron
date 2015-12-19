#include <functional>
#include <iostream>

#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "grouping/grouping.h"
#include "grouping/all-grouping.h"

namespace heron {
namespace stmgr {

AllGrouping::AllGrouping(const std::vector<sp_int32>& _task_ids)
    : Grouping(_task_ids) {}

AllGrouping::~AllGrouping() {}

void AllGrouping::GetListToSend(const proto::system::HeronDataTuple&,
                                std::list<sp_int32>& _return) {
  for (sp_uint32 i = 0; i < task_ids_.size(); ++i) {
    _return.push_back(task_ids_[i]);
  }
}

}  // namespace stmgr
}  // namespace heron
