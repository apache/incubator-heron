#ifndef STMGR_REGISTER_PROCESSOR_H_
#define STMGR_REGISTER_PROCESSOR_H_

namespace heron { namespace tmaster {

class StMgrRegisterProcessor : public Processor {
 public:
  StMgrRegisterProcessor(REQID _reqid,
                        Connection* _conn,
                        proto::tmaster::StMgrRegisterRequest* _request,
                        TMaster* _tmaster,
                        Server* _server);
  virtual ~StMgrRegisterProcessor();

  void Start();
};

}}
#endif
