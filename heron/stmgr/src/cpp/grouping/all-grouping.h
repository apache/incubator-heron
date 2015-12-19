#ifndef SRC_CPP_SVCS_STMGR_SRC_GROUPING_ALL_GROUPING_H_
#define SRC_CPP_SVCS_STMGR_SRC_GROUPING_ALL_GROUPING_H_

#include <list>
#include <vector>

namespace heron {
namespace stmgr {

class AllGrouping : public Grouping {
 public:
  explicit AllGrouping(const std::vector<sp_int32>& _task_ids);
  virtual ~AllGrouping();

  virtual void GetListToSend(const proto::system::HeronDataTuple& _tuple,
                             std::list<sp_int32>& _return);
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_GROUPING_ALL_GROUPING_H_
