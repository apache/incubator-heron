#ifndef SRC_CPP_SVCS_STMGR_SRC_UTIL_XOR_MANAGER_H_
#define SRC_CPP_SVCS_STMGR_SRC_UTIL_XOR_MANAGER_H_

#include <map>
#include <vector>

namespace heron {
namespace stmgr {

class RotatingMap;

class XorManager {
 public:
  XorManager(EventLoop* eventLoop, sp_int32 _timeout,
             const std::vector<sp_int32>& _task_ids);
  virtual ~XorManager();

  // Create a new entry for the tuple.
  // _task_id is the task id where the tuple
  // originated from.
  // _key is the tuple key
  // _value is the tuple key as seen by the
  // destination
  void create(sp_int32 _task_id, sp_int64 _key, sp_int64 _value);

  // Add one more entry to the tuple tree
  // _task_id is the task id where the tuple
  // originated from.
  // _key is the tuple key
  // _value is the tuple key as seen by the
  // destination
  // We return true if the xor value is now zerod out
  // Else return false
  bool anchor(sp_int32 _task_id, sp_int64 _key, sp_int64 _value);

  // remove this tuple key from our structure.
  // return true if this key was found. else false
  bool remove(sp_int32 _task_id, sp_int64 _key);

 private:
  void rotate(EventLoopImpl::Status _status);

  EventLoop* eventLoop_;
  sp_int32 timeout_;

  // map of task_id to a RotatingMap
  std::map<sp_int32, RotatingMap*> tasks_;

  // Configs to be read
  sp_int32 n_buckets_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_UTIL_XOR_MANAGER_H_
