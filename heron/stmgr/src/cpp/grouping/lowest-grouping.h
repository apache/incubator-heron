#ifndef SRC_CPP_SVCS_STMGR_SRC_GROUPING_LOWEST_GROUPING_H_
#define SRC_CPP_SVCS_STMGR_SRC_GROUPING_LOWEST_GROUPING_H_

#include <list>
#include <vector>

namespace heron {
namespace stmgr {

class LowestGrouping : public Grouping {
 public:
  explicit LowestGrouping(const std::vector<sp_int32>& _task_ids);
  virtual ~LowestGrouping();

  virtual void GetListToSend(const proto::system::HeronDataTuple& _tuple,
                             std::list<sp_int32>& _return);

 private:
  sp_int32 lowest_taskid_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_GROUPING_LOWEST_GROUPING_H_
