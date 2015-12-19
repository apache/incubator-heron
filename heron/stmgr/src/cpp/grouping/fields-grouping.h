#ifndef SRC_CPP_SVCS_STMGR_SRC_GROUPING_FIELDS_GROUPING_H_
#define SRC_CPP_SVCS_STMGR_SRC_GROUPING_FIELDS_GROUPING_H_

#include <functional>
#include <list>
#include <vector>

namespace heron {
namespace stmgr {

class FieldsGrouping : public Grouping {
 public:
  FieldsGrouping(const proto::api::InputStream& _is,
                 const proto::api::StreamSchema& _schema,
                 const std::vector<sp_int32>& _task_ids);
  virtual ~FieldsGrouping();

  virtual void GetListToSend(const proto::system::HeronDataTuple& _tuple,
                             std::list<sp_int32>& _return);

 private:
  std::list<sp_int32> fields_grouping_indices_;
  std::hash<sp_string> str_hash_fn;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_GROUPING_FIELDS_GROUPING_H_
