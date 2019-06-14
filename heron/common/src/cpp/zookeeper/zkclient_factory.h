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

#ifndef SRC_CPP_CORE_ZK_PUBLIC_ZKCLIENT_FACTORY_H_
#define SRC_CPP_CORE_ZK_PUBLIC_ZKCLIENT_FACTORY_H_

#include <string>
#include "zookeeper/zkclient.h"
#include "basics/basics.h"
#include "network/network.h"

class ZKClientFactory {
 public:
  virtual ZKClient* create(const std::string& hostportlist, std::shared_ptr<EventLoop> eventLoop,
                           VCallback<ZKClient::ZkWatchEvent> global_watcher_cb) = 0;

  virtual ~ZKClientFactory() {}
};

class DefaultZKClientFactory : public ZKClientFactory {
 public:
  virtual ZKClient* create(const std::string& hostportlist, std::shared_ptr<EventLoop> eventLoop,
                           VCallback<ZKClient::ZkWatchEvent> global_watcher_cb) {
    if (global_watcher_cb == NULL) {
      return new ZKClient(hostportlist, eventLoop);
    } else {
      return new ZKClient(hostportlist, eventLoop, global_watcher_cb);
    }
  }

  virtual ~DefaultZKClientFactory() {}
};

#endif  // SRC_CPP_CORE_ZK_PUBLIC_ZKCLIENT_FACTORY_H_
