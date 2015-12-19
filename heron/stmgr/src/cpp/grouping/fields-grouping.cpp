#include <iostream>

#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "grouping/grouping.h"
#include "grouping/fields-grouping.h"

namespace heron {
namespace stmgr {

FieldsGrouping::FieldsGrouping(const proto::api::InputStream& _is,
                               const proto::api::StreamSchema& _schema,
                               const std::vector<sp_int32>& _task_ids)
    : Grouping(_task_ids) {
  for (sp_int32 i = 0; i < _schema.keys_size(); ++i) {
    for (sp_int32 j = 0; j < _is.grouping_fields().keys_size(); ++j) {
      if (_schema.keys(i).key() == _is.grouping_fields().keys(j).key()) {
        fields_grouping_indices_.push_back(i);
        break;
      }
    }
  }
}

FieldsGrouping::~FieldsGrouping() {}

void FieldsGrouping::GetListToSend(const proto::system::HeronDataTuple& _tuple,
                                   std::list<sp_int32>& _return) {
  sp_int32 task_index = 0;
  size_t prime_num = 633910111UL;
  for (std::list<sp_int32>::iterator iter = fields_grouping_indices_.begin();
       iter != fields_grouping_indices_.end(); ++iter) {
    CHECK(_tuple.values_size() > *iter);
    size_t h = str_hash_fn(_tuple.values(*iter));
    task_index += (h % prime_num);
  }
  task_index = task_index % task_ids_.size();
  _return.push_back(task_ids_[task_index]);
}

}  // namespace stmgr
}  // namespace heron
