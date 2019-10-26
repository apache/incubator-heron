/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

#ifndef __ECHOCLIENT_H
#define __ECHOCLIENT_H

#include "core/network/misc/tests.pb.h"
#include "core/network/public/event_loop_impl.h"
#include "core/network/public/client.h"
#include "core/network/public/networkoptions.h"
#include "network/network_error.h"
#include "core/common/public/sptypes.h"

class EchoClient : public Client
{
 public:
  EchoClient(EventLoopImpl* ss, const NetworkOptions& options, bool _perf);
  ~EchoClient();

 protected:
  virtual void HandleConnect(NetworkErrorCode status);
  virtual void HandleClose(NetworkErrorCode status);

 private:
  void HandleEchoResponse(void*, std::unique_ptr<EchoServerResponse> response,
                          NetworkErrorCode status);
  void CreateAndSendRequest();
  sp_int32 nrequests_;
  bool perf_;
};

#endif
