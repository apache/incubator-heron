#include <functional>
#include <iostream>

#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "grouping/grouping.h"
#include "grouping/shuffle-grouping.h"

namespace heron {
namespace stmgr {

ShuffleGrouping::ShuffleGrouping(const std::vector<sp_int32>& _task_ids)
    : Grouping(_task_ids) {
  next_index_ = rand() % task_ids_.size();
}

ShuffleGrouping::~ShuffleGrouping() {}

void ShuffleGrouping::GetListToSend(const proto::system::HeronDataTuple&,
                                    std::list<sp_int32>& _return) {
  _return.push_back(task_ids_[next_index_]);
  next_index_ = (next_index_ + 1) % task_ids_.size();
}

}  // namespace stmgr
}  // namespace heron
