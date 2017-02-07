/********************************************************************
 * 2014 - 
 * open source under Apache License Version 2.0
 ********************************************************************/
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef _HDFS_LIBHDFS3_MOCK_MOCKRPCREMOTECALL_H_
#define _HDFS_LIBHDFS3_MOCK_MOCKRPCREMOTECALL_H_

#include "gmock/gmock.h"
#include "rpc/RpcRemoteCall.h"

using namespace Hdfs::Internal;
namespace Hdfs {
namespace Mock {

class MockRpcRemoteCall: public RpcRemoteCall {
public:
	MockRpcRemoteCall(const RpcCall & c, int32_t id, const std::string & clientId) :
			RpcRemoteCall(c, id, clientId) {
	}

	MOCK_METHOD2(serialize, void(const RpcProtocolInfo&, WriteBuffer &));
	MOCK_METHOD1(cancel, void(exception_ptr ));

};

}
}

#endif /* _HDFS_LIBHDFS3_MOCK_MOCKRPCREMOTECALL_H_ */
