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
// This file defines the AsyncDNS class. AsyncDNS is used by processes resolve
// DNS addresses asynchronously. Currently, we use libevent's evdns mechanism
// underneath but its conceavable to use other implementations. Right now the
// only implementation is for http clients.
//
///////////////////////////////////////////////////////////////////////////////

#ifndef ASYNCDNS_H_
#define ASYNCDNS_H_

#include <functional>
#include <vector>
#include "network/event_loop.h"
#include "basics/basics.h"

// Forward declaration
struct evdns_base;

/*
 * AsyncDNS class definition
 */
class AsyncDNS {
 public:
  // User supplied callbacks are supplied with a status upon invokation.
  enum Status {
    // Uknown event happened. This in theory should not happen
    OK = 0,
    // A file descriptor is ready for read
    NOTOK,
  };

  struct Result {
    Status status_;
    std::vector<sp_int32> addr_;
  };

  // Constructor/Destructor
  explicit AsyncDNS(std::shared_ptr<EventLoop> eventLoop);
  ~AsyncDNS();

  // The main interface function.
  // Resolve the address
  sp_int32 Resolve(const sp_string& _address, VCallback<Result> _cb);

  //! Accessor function
  struct evdns_base* dns() {
    return dns_;
  }

 private:
  // The underlying dispatcher that we wrap around.
  struct evdns_base* dns_;
};

#endif  // ASYNCDNS_H_
