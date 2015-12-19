#include <functional>
#include <iostream>

#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "grouping/grouping.h"
#include "grouping/custom-grouping.h"

namespace heron {
namespace stmgr {

CustomGrouping::CustomGrouping(const std::vector<sp_int32>& _task_ids)
    : Grouping(_task_ids) {}

CustomGrouping::~CustomGrouping() {}

void CustomGrouping::GetListToSend(const proto::system::HeronDataTuple&,
                                   std::list<sp_int32>&) {
  // Stmgr does not do the custom grouping.
  // That is done by the instance
  return;
}

}  // namespace stmgr
}  // namespace heron
