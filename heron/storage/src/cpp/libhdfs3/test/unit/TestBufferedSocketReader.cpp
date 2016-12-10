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
#include "Exception.h"
#include "ExceptionInternal.h"
#include "gtest/gtest.h"
#include "MockSocket.h"
#include "network/BufferedSocketReader.h"
#include "TestUtil.h"
#include "Thread.h"

using namespace Hdfs;
using namespace Hdfs::Internal;
using namespace testing;

class TestBufferedSocketReader: public ::testing::Test {
public:
    TestBufferedSocketReader() :
        reader(sock) {
    }

public:
    MockSocket sock;
    BufferedSocketReaderImpl reader;
};

static int32_t ReadAction(char * buffer, int32_t size, const char * target,
                          int32_t tsize) {
    int32_t todo = size < tsize ? size : tsize;
    memcpy(buffer, target, todo);
    return todo;
}

static void ReadFullyAction(char * buffer, int32_t size, int timeout,
                            const char * target, int32_t tsize) {
    assert(size == tsize);
    memcpy(buffer, target, size);
}

TEST_F(TestBufferedSocketReader, TestRead) {
    ASSERT_EQ(0, reader.size);
    ASSERT_EQ(0, reader.cursor);
    char readData[64];
    int rsize = sizeof(readData);
    EXPECT_CALL(sock, read(_, _)).Times(1).WillOnce(
        Invoke(bind(&ReadAction, _1, _2, readData, rsize)));
    /*
     * fill the buffer, read from buffer.
     */
    int bufferSize = reader.buffer.size();
    ASSERT_GT(bufferSize, 0);
    FillBuffer(&reader.buffer[0], bufferSize, 0);
    reader.size = bufferSize;
    std::vector<char> target(bufferSize);
    ASSERT_EQ(1, reader.read(&target[0], 1));
    ASSERT_TRUE(CheckBuffer(&target[0], 1, 0));
    ASSERT_EQ(bufferSize - 1, reader.read(&target[1], bufferSize - 1));
    ASSERT_TRUE(CheckBuffer(&target[0], bufferSize, 0));
    /*
     * Refill the buffer, read cross the buffer.
     */
    reader.cursor = 0;
    reader.size = bufferSize;
    target.resize(bufferSize + sizeof(readData));
    FillBuffer(readData, sizeof(readData), bufferSize);
    int32_t todo = target.size();

    while (todo > 0) {
        todo -= reader.read(&target[0] + (target.size() - todo), todo);
    }

    ASSERT_TRUE(CheckBuffer(&target[0], target.size(), 0));
    ASSERT_EQ(0, reader.size);
    ASSERT_EQ(0, reader.cursor);
}

TEST_F(TestBufferedSocketReader, TestReadFully) {
    ASSERT_EQ(0, reader.size);
    ASSERT_EQ(0, reader.cursor);
    char readData[64];
    int rsize = sizeof(readData);
    EXPECT_CALL(sock, readFully(_, _, 500)).Times(1).WillOnce(
        Invoke(bind(&ReadFullyAction, _1, _2, _3, readData, rsize)));
    /*
     * fill the buffer, read from buffer.
     */
    int bufferSize = reader.buffer.size();
    ASSERT_GT(bufferSize, 0);
    FillBuffer(&reader.buffer[0], bufferSize, 0);
    reader.size = bufferSize;
    std::vector<char> target(bufferSize);
    reader.readFully(&target[0], 1, 500);
    ASSERT_TRUE(CheckBuffer(&target[0], 1, 0));
    reader.readFully(&target[1], bufferSize - 1, 500);
    ASSERT_TRUE(CheckBuffer(&target[0], bufferSize, 0));
    /*
     * Refill the buffer, read cross the buffer.
     */
    reader.cursor = 0;
    reader.size = bufferSize;
    target.resize(bufferSize + sizeof(readData));
    FillBuffer(readData, sizeof(readData), bufferSize);
    reader.readFully(&target[0], target.size(), 500);
    ASSERT_TRUE(CheckBuffer(&target[0], target.size(), 0));
    ASSERT_EQ(0, reader.size);
    ASSERT_EQ(0, reader.cursor);
}

TEST_F(TestBufferedSocketReader, TestBigEndianInt32) {
    ASSERT_EQ(0, reader.size);
    ASSERT_EQ(0, reader.cursor);
    ASSERT_GE(reader.buffer.size(), sizeof(int32_t));
    reader.buffer[0] = '1';
    reader.buffer[1] = '2';
    reader.buffer[2] = '3';
    reader.buffer[3] = '4';
    reader.size = 4;
    char target[4] = { '4', '3', '2', '1' };
    EXPECT_EQ(*reinterpret_cast<int32_t *>(target),
              reader.readBigEndianInt32(500));
}

TEST_F(TestBufferedSocketReader, TestReadVarInt32) {
    ASSERT_EQ(0, reader.size);
    ASSERT_EQ(0, reader.cursor);
    ASSERT_GE(reader.buffer.size(), sizeof(int32_t));
    reader.buffer[0] = 0;
    reader.buffer[1] = 1;
    reader.buffer[2] = 2;
    reader.size = 3;
    reader.cursor = 1;
    char target[2];
    target[0] = 1;
    target[1] = 2;
    EXPECT_CALL(sock, read(_, _)).Times(1).WillOnce(
        Invoke(bind(&ReadAction, _1, _2, target, sizeof(target))));
    EXPECT_CALL(sock, poll(true, false, _)).WillOnce(Return(true));
    EXPECT_EQ(1, reader.readVarint32(500));
    EXPECT_EQ(1, reader.size - reader.cursor);
    EXPECT_EQ(2, reader.readVarint32(500));
    EXPECT_EQ(0, reader.size - reader.cursor);
    EXPECT_EQ(1, reader.readVarint32(500));
    EXPECT_EQ(2, reader.readVarint32(500));
    EXPECT_EQ(0, reader.size - reader.cursor);
}

TEST_F(TestBufferedSocketReader, TestReadVarInt32_Failure) {
    EXPECT_CALL(sock, poll(true, false, _)).Times(AnyNumber()).WillOnce(
        Return(false)).WillRepeatedly(Return(true));
    char target[1];
    target[0] = -1;
    EXPECT_CALL(sock, read(_, _)).Times(AnyNumber()).WillRepeatedly(
        Invoke(bind(&ReadAction, _1, _2, target, sizeof(target))));
    EXPECT_THROW(reader.readVarint32(0), HdfsTimeoutException);
    EXPECT_THROW(reader.readVarint32(1000), HdfsNetworkException);
}
