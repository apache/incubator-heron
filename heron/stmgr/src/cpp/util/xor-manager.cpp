#include <unordered_map>

#include <iostream>

#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "config/heron-internals-config-reader.h"

#include "util/rotating-map.h"
#include "util/xor-manager.h"

namespace heron {
namespace stmgr {

XorManager::XorManager(EventLoop* eventLoop, sp_int32 _timeout,
                       const std::vector<sp_int32>& _task_ids)
    : eventLoop_(eventLoop), timeout_(_timeout) {
  n_buckets_ = config::HeronInternalsConfigReader::Instance()
                   ->GetHeronStreammgrXormgrRotatingmapNbuckets();

  eventLoop_->registerTimer([this] (EventLoop::Status status) { this->rotate(status); },
      false, _timeout * 1000000);
  std::vector<sp_int32>::const_iterator iter;
  for (iter = _task_ids.begin(); iter != _task_ids.end(); ++iter) {
    tasks_[*iter] = new RotatingMap(n_buckets_);
  }
}

XorManager::~XorManager() {
  std::map<sp_int32, RotatingMap*>::iterator iter;
  for (iter = tasks_.begin(); iter != tasks_.end(); ++iter) {
    delete iter->second;
  }
}

void XorManager::rotate(EventLoopImpl::Status) {
  std::map<sp_int32, RotatingMap*>::iterator iter;
  for (iter = tasks_.begin(); iter != tasks_.end(); ++iter) {
    iter->second->rotate();
  }
  sp_int32 timeout = timeout_ / n_buckets_ + timeout_ % n_buckets_;
  eventLoop_->registerTimer([this] (EventLoop::Status status) { this->rotate(status); },
      false, timeout * 1000000);
}

void XorManager::create(sp_int32 _task_id, sp_int64 _key, sp_int64 _value) {
  CHECK(tasks_.find(_task_id) != tasks_.end());
  tasks_[_task_id]->create(_key, _value);
}

bool XorManager::anchor(sp_int32 _task_id, sp_int64 _key, sp_int64 _value) {
  CHECK(tasks_.find(_task_id) != tasks_.end());
  return tasks_[_task_id]->anchor(_key, _value);
}

bool XorManager::remove(sp_int32 _task_id, sp_int64 _key) {
  CHECK(tasks_.find(_task_id) != tasks_.end());
  return tasks_[_task_id]->remove(_key);
}

}  // namespace stmgr
}  // namespace heron
