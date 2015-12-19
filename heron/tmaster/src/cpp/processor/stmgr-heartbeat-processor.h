#ifndef STMGR_HEARTBEAT_PROCESSOR_
#define STMGR_HEARTBEAT_PROCESSOR_

namespace heron { namespace tmaster {

class StMgrHeartbeatProcessor : public Processor {
 public:
  StMgrHeartbeatProcessor(REQID _reqid,
                          Connection* _conn,
                          proto::tmaster::StMgrHeartbeatRequest* _request,
                          TMaster* _tmaster,
                          Server* _server);
  virtual ~StMgrHeartbeatProcessor();

  void Start();
};

}}
#endif
