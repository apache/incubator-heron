#ifndef SRC_CPP_SVCS_STMGR_SRC_GROUPING_GROUPING_H_
#define SRC_CPP_SVCS_STMGR_SRC_GROUPING_GROUPING_H_

#include <list>
#include <vector>

namespace heron {
namespace stmgr {

class Grouping {
 public:
  explicit Grouping(const std::vector<sp_int32>& _task_ids);
  virtual ~Grouping();

  static Grouping* Create(proto::api::Grouping grouping_,
                          const proto::api::InputStream& _is,
                          const proto::api::StreamSchema& _schema,
                          const std::vector<sp_int32>& _task_ids);

  virtual void GetListToSend(const proto::system::HeronDataTuple& _tuple,
                             std::list<sp_int32>& _return) = 0;

 protected:
  std::vector<sp_int32> task_ids_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_GROUPING_GROUPING_H_
