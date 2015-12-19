#ifndef SRC_CPP_SVCS_STMGR_SRC_MANAGER_STREAM_CONSUMERS_H_
#define SRC_CPP_SVCS_STMGR_SRC_MANAGER_STREAM_CONSUMERS_H_

#include <list>
#include <vector>

namespace heron {
namespace stmgr {

class Grouping;

class StreamConsumers {
 public:
  StreamConsumers(const proto::api::InputStream& _is,
                  const proto::api::StreamSchema& _schema,
                  const std::vector<sp_int32>& _task_ids);
  virtual ~StreamConsumers();

  void NewConsumer(const proto::api::InputStream& _is,
                   const proto::api::StreamSchema& _schema,
                   const std::vector<sp_int32>& _task_ids);

  void GetListToSend(const proto::system::HeronDataTuple& _tuple,
                     std::list<sp_int32>& _return);

 private:
  std::list<Grouping*> consumers_;
};

}  // namespace stmgr
}  // namespace heron

#endif  // SRC_CPP_SVCS_STMGR_SRC_MANAGER_STREAM_CONSUMERS_H_
