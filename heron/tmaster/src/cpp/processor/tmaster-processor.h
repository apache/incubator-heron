#ifndef TMASTER_PROCESSOR_H_
#define TMASTER_PROCESSOR_H_

namespace heron { namespace tmaster {

class TMaster;

class Processor {
 public:
  Processor(REQID _reqid,
            Connection* _conn,
            google::protobuf::Message* _request,
            TMaster* _tmaster,
            Server* _server);
  virtual ~Processor();
  virtual void Start() = 0;

 protected:
  void SendResponse(const google::protobuf::Message& _response);
  Connection* GetConnection() { return conn_; }
  void CloseConnection();
  google::protobuf::Message*        request_;
  TMaster*                          tmaster_;
  Server*                           server_;

 private:
  REQID                             reqid_;
  Connection*                       conn_;
};

}}
#endif
