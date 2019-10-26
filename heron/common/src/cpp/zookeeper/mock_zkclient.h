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

#ifndef SRC_CPP_CORE_ZK_PUBLIC_MOCK_ZKCLIENT_H_
#define SRC_CPP_CORE_ZK_PUBLIC_MOCK_ZKCLIENT_H_

#include <string>
#include <vector>

#include "gmock/gmock.h"
#include "glog/logging.h"
#include "zookeeper/zkclient.h"
#include "network/network.h"
#include "basics/basics.h"

class MockZKClient : public ZKClient {
 public:
  MOCK_METHOD3(Exists,
               void(const std::string& _node, VCallback<> _watcher, VCallback<sp_int32> _cb));
  MOCK_METHOD2(Exists, void(const std::string& _node, VCallback<sp_int32> _cb));
  MOCK_METHOD4(CreateNode, void(const std::string& _node, const std::string& _value,
                                bool _is_ephimeral, VCallback<sp_int32> _cb));
  MOCK_METHOD2(DeleteNode, void(const std::string& _node, VCallback<sp_int32> _cb));
  MOCK_METHOD5(Get, void(const std::string& _node, std::string* _data, sp_int32* _version,
                         VCallback<> _watcher, VCallback<sp_int32> _cb));
  MOCK_METHOD4(Get, void(const std::string& _node, std::string* _data, sp_int32* _version,
                         VCallback<sp_int32> _cb));
  MOCK_METHOD3(Get, void(const std::string& _node, std::string* _data, VCallback<sp_int32> _cb));
  MOCK_METHOD4(Set, void(const std::string& _node, const std::string& _data, sp_int32 _version,
                         VCallback<sp_int32> _cb));
  MOCK_METHOD3(Set,
               void(const std::string& _node, const std::string& _data, VCallback<sp_int32> _cb));
  MOCK_METHOD3(GetChildren, void(const std::string& _node, std::vector<std::string>* _children,
                                 VCallback<sp_int32> _cb));
  MOCK_METHOD0(Die, void());
  virtual ~MockZKClient() { Die(); }
};

#endif  // SRC_CPP_CORE_ZK_PUBLIC_MOCK_ZKCLIENT_H_
