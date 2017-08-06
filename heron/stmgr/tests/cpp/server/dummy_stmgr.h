/*
 * Copyright 2015 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef __DUMMY_STMGR_H
#define __DUMMY_STMGR_H

#include <vector>
#include "network/network_error.h"

class DummyTMasterClient : public Client {
 public:
  DummyTMasterClient(EventLoopImpl* eventLoop, const NetworkOptions& _options,
                     const sp_string& stmgr_id, const sp_string& stmgr_host, sp_int32 stmgr_port,
                     sp_int32 shell_port,
                     const std::vector<heron::proto::system::Instance*>& instances);
  virtual ~DummyTMasterClient();

 private:
  void Retry() { Start(); }
  // Handle incoming connections
  virtual void HandleConnect(NetworkErrorCode _status);
  // Handle connection close
  virtual void HandleClose(NetworkErrorCode _status);
  virtual void HandleRegisterResponse(void*, heron::proto::tmaster::StMgrRegisterResponse* response,
                                      NetworkErrorCode);
  // Send worker request
  void CreateAndSendRegisterRequest();

 private:
  VCallback<> retry_cb_;
  sp_string stmgr_id_;
  sp_string stmgr_host_;
  sp_int32 stmgr_port_;
  sp_int32 shell_port_;
  std::vector<heron::proto::system::Instance*> instances_;
};

class DummyStMgr : public Server {
 public:
  DummyStMgr(EventLoopImpl* ss, const NetworkOptions& options, const sp_string& stmgr_id,
             const sp_string& stmgr_host, sp_int32 stmgr_port, const sp_string& tmaster_host,
             sp_int32 tmaster_port, sp_int32 shell_port,
             const std::vector<heron::proto::system::Instance*>& instances);

  virtual ~DummyStMgr();
  sp_int32 NumStartBPMsgs() { return num_start_bp_; }
  sp_int32 NumStopBPMsgs() { return num_stop_bp_; }
  std::vector<sp_string>& OtherStmgrsIds() { return other_stmgrs_ids_; }

 protected:
  // handle an incoming connection from server
  virtual void HandleNewConnection(Connection* newConnection);

  // handle a connection close
  virtual void HandleConnectionClose(Connection* connection, NetworkErrorCode status);

  // Handle st mgr hello message
  virtual void HandleStMgrHelloRequest(REQID _id, Connection* _conn,
                                       heron::proto::stmgr::StrMgrHelloRequest* _request);
  virtual void HandleStartBackPressureMessage(Connection*,
                                              heron::proto::stmgr::StartBackPressureMessage*);
  virtual void HandleStopBackPressureMessage(Connection*,
                                             heron::proto::stmgr::StopBackPressureMessage*);

 private:
  std::vector<sp_string> other_stmgrs_ids_;
  sp_int32 num_start_bp_;
  sp_int32 num_stop_bp_;
  DummyTMasterClient* tmaster_client_;
};

#endif
