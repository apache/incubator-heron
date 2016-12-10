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
#ifndef _HDFS_LIBHDFS3_MOCK_MOCKBUFFEREDSOCKETREADER_H_
#define _HDFS_LIBHDFS3_MOCK_MOCKBUFFEREDSOCKETREADER_H_

#include "gmock/gmock.h"
#include "network/BufferedSocketReader.h"

namespace Hdfs {
namespace Mock {

class MockBufferedSocketReader: public Hdfs::Internal::BufferedSocketReader {
public:
	MOCK_METHOD2(read, int32_t(char * b, int32_t s));
	MOCK_METHOD3(readFully, void(char * b, int32_t s, int timeout));
	MOCK_METHOD1(readBigEndianInt32, int32_t(int timeout));
	MOCK_METHOD1(readVarint32, int32_t(int timeout));
	MOCK_METHOD1(poll, bool(int timeout));
};

}
}

#endif /* _HDFS_LIBHDFS3_MOCK_MOCKBUFFEREDSOCKETREADER_H_ */
