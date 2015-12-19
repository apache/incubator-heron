#include <iostream>

#include "proto/messages.h"

#include "basics/basics.h"
#include "errors/errors.h"
#include "threads/threads.h"
#include "network/network.h"

#include "util/rotating-map.h"

namespace heron {
namespace stmgr {

RotatingMap::RotatingMap(sp_int32 _nbuckets) {
  for (sp_int32 i = 0; i < _nbuckets; ++i) {
    std::unordered_map<sp_int64, sp_int64>* head =
        new std::unordered_map<sp_int64, sp_int64>();
    buckets_.push_back(head);
  }
}

RotatingMap::~RotatingMap() {
  while (!buckets_.empty()) {
    std::unordered_map<sp_int64, sp_int64>* m = buckets_.front();
    buckets_.pop_front();
    delete m;
  }
}

void RotatingMap::rotate() {
  std::unordered_map<sp_int64, sp_int64>* m = buckets_.back();
  buckets_.pop_back();
  delete m;
  buckets_.push_front(new std::unordered_map<sp_int64, sp_int64>());
}

void RotatingMap::create(sp_int64 _key, sp_int64 _value) {
  std::unordered_map<sp_int64, sp_int64>* m = buckets_.front();
  (*m)[_key] = _value;
}

bool RotatingMap::anchor(sp_int64 _key, sp_int64 _value) {
  std::list<std::unordered_map<sp_int64, sp_int64>*>::iterator iter;
  for (iter = buckets_.begin(); iter != buckets_.end(); ++iter) {
    std::unordered_map<sp_int64, sp_int64>* m = *iter;
    if (m->find(_key) != m->end()) {
      sp_int64 current_value = (*m)[_key];
      sp_int64 new_value = current_value ^ _value;
      (*m)[_key] = new_value;
      return new_value == 0;
    }
  }
  return false;
}

bool RotatingMap::remove(sp_int64 _key) {
  std::list<std::unordered_map<sp_int64, sp_int64>*>::iterator iter;
  for (iter = buckets_.begin(); iter != buckets_.end(); ++iter) {
    size_t removed = (*iter)->erase(_key);
    if (removed > 0) return true;
  }
  return false;
}

}  // namespace stmgr
}  // namespace heron
