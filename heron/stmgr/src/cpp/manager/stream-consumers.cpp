#include <functional>
#include <iostream>

#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "grouping/grouping.h"
#include "manager/stream-consumers.h"

namespace heron {
namespace stmgr {

StreamConsumers::StreamConsumers(const proto::api::InputStream& _is,
                                 const proto::api::StreamSchema& _schema,
                                 const std::vector<sp_int32>& _task_ids) {
  consumers_.push_back(Grouping::Create(_is.gtype(), _is, _schema, _task_ids));
}

StreamConsumers::~StreamConsumers() {
  while (!consumers_.empty()) {
    Grouping* c = consumers_.front();
    consumers_.pop_front();
    delete c;
  }
}

void StreamConsumers::NewConsumer(const proto::api::InputStream& _is,
                                  const proto::api::StreamSchema& _schema,
                                  const std::vector<sp_int32>& _task_ids) {
  consumers_.push_back(Grouping::Create(_is.gtype(), _is, _schema, _task_ids));
}

void StreamConsumers::GetListToSend(const proto::system::HeronDataTuple& _tuple,
                                    std::list<sp_int32>& _return) {
  std::list<Grouping*>::iterator iter = consumers_.begin();
  for (; iter != consumers_.end(); ++iter) {
    (*iter)->GetListToSend(_tuple, _return);
  }
}

}  // namespace stmgr
}  // namespace heron
