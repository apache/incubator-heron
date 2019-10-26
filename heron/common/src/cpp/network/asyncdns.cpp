/**
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

///////////////////////////////////////////////////////////////////////////////
//
// Implements the AsyncDNS. Currently dummy.
//
///////////////////////////////////////////////////////////////////////////////
#include "network/asyncdns.h"
#include <evdns.h>
#include "glog/logging.h"
#include "basics/basics.h"

// Constructor. We create a new event_base.
AsyncDNS::AsyncDNS(std::shared_ptr<EventLoop> eventLoop) {
    dns_ = evdns_base_new(eventLoop->dispatcher(), 1);
}

// Destructor.
AsyncDNS::~AsyncDNS() { evdns_base_free(dns_, 1); }

sp_int32 AsyncDNS::Resolve(const sp_string&, VCallback<AsyncDNS::Result>) {
  CHECK(false) << "DNS resolve not implemented";
  return SP_NOTOK;
}
