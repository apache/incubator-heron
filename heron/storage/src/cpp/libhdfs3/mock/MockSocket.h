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
#ifndef _HDFS_LIBHDFS3_MOCK_MOCKSOCKET_H_
#define _HDFS_LIBHDFS3_MOCK_MOCKSOCKET_H_

#include "gmock/gmock.h"

#include "network/Socket.h"

class MockSocket: public Hdfs::Internal::Socket {
public:

	MOCK_METHOD2(read, int32_t(char * buffer, int32_t size));

	MOCK_METHOD3(readFully, void(char * buffer, int32_t size, int timeout));

	MOCK_METHOD2(write, int32_t(const char * buffer, int32_t size));

	MOCK_METHOD3(writeFully, void(const char * buffer, int32_t size, int timeout));

	MOCK_METHOD3(connect, void(const char * host, int port, int timeout));

	MOCK_METHOD3(connect, void(const char * host, const char * port, int timeout));

	MOCK_METHOD4(connect, void(struct addrinfo * paddr, const char * host, const char * port,
					int timeout));

	MOCK_METHOD3(poll, bool(bool read, bool write, int timeout));

	MOCK_METHOD1(setBlockMode, void(bool enable));

	MOCK_METHOD1(setNoDelay, void(bool enable));

	MOCK_METHOD1(setLingerTimeout, void(int timeout));

	MOCK_METHOD0(disableSigPipe, void());

	MOCK_METHOD0(close, void());
};

#endif /* _HDFS_LIBHDFS3_MOCK_MOCKSOCKET_H_ */
