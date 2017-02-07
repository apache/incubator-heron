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
#include "gtest/gtest.h"
#include "gmock/gmock.h"

#include "client/InputStreamImpl.h"
#include "client/FileSystem.h"
#include "MockFileSystemInter.h"
#include "TestDatanodeStub.h"
#include "MockDatanode.h"
#include "server/ExtendedBlock.h"
#include "XmlConfig.h"
#include <string>

using namespace Hdfs::Internal;
using namespace Hdfs::Mock;
using namespace testing;
using ::testing::AtLeast;

class TestInputSteam: public ::testing::Test {

public:
    TestInputSteam() {
    }
    ~TestInputSteam() {
    }

protected:
    MockFileSystemInter fs;
    InputStreamImpl ins;
};

class MockLocatedBlocks: public LocatedBlocks {
public:
    MOCK_CONST_METHOD0(getFileLength, int64_t ());
    MOCK_METHOD1(setFileLength, void (int64_t fileLength));
    MOCK_CONST_METHOD0(isLastBlockComplete, bool ());
    MOCK_METHOD1(setIsLastBlockComplete, void (bool lastBlockComplete));
    MOCK_METHOD0(getLastBlock, shared_ptr<LocatedBlock> ());
    MOCK_METHOD1(setLastBlock, void (const LocatedBlock & lastBlock));
    MOCK_CONST_METHOD0(isUnderConstruction, bool ());
    MOCK_METHOD1(setUnderConstruction, void (bool underConstruction));
    MOCK_METHOD1(findBlock, LocatedBlock * (int64_t position));
    MOCK_METHOD0(getBlocks, std::vector<LocatedBlock> & ());
    MOCK_METHOD1(setLastBlock, void(shared_ptr<LocatedBlock>));
};

class MockDatanodeStub: public TestDatanodeStub {
public:
    MOCK_METHOD0(getDatanode, shared_ptr<MockDatanode>());

};

TEST(InputStreamTest, ReadBlockLength_Success) {
    InputStreamImpl ins;
    LocatedBlock b;
    shared_ptr<MockDatanode> datanode(new MockDatanode());
    MockDatanodeStub stub;
    std::vector<DatanodeInfo> dfv;
    DatanodeInfo df;
    dfv.push_back(df);
    b.locs = dfv;
    ins.stub = &stub;
    EXPECT_CALL(stub, getDatanode()).Times(1).WillOnce(Return(datanode));
    EXPECT_CALL(*datanode, getReplicaVisibleLength(_)).Times(1).WillOnce(Return(2));
    EXPECT_NO_THROW(ins.readBlockLength(b));
}

TEST(InputStreamTest, ReadBlockLength_Fail) {
    InputStreamImpl ins;
    shared_ptr<MockDatanode> datanode(new MockDatanode());
    MockDatanodeStub stub;
    LocatedBlock b;
    std::vector<DatanodeInfo> dfv;
    DatanodeInfo df1;
    dfv.push_back(df1);
    b.locs = dfv;
    ins.stub = &stub;
    EXPECT_CALL(stub, getDatanode()).Times(1).WillOnce(Return(datanode));
    EXPECT_CALL(*datanode, getReplicaVisibleLength(_)).Times(1).WillOnce(Return(-1));
    EXPECT_EQ(ins.readBlockLength(b), -1);
}

TEST(InputStreamTest, ReadBlockLength_FailTry) {
    InputStreamImpl ins;
    shared_ptr<MockDatanode> datanode(new MockDatanode());
    MockDatanodeStub stub;
    LocatedBlock b;
    std::vector<DatanodeInfo> dfv;
    DatanodeInfo df1;
    DatanodeInfo df2;
    dfv.push_back(df1);
    dfv.push_back(df2);
    b.locs = dfv;
    ins.stub = &stub;
    EXPECT_CALL(stub, getDatanode()).Times(2).WillOnce(Return(datanode)).WillOnce(Return(datanode));
    EXPECT_CALL(*datanode, getReplicaVisibleLength(_)).Times(2).WillOnce(Return(-1)).WillOnce(
        Return(0));
    EXPECT_EQ(ins.readBlockLength(b), 0);
}

TEST(InputStreamTest, ReadBlockLength_ThrowTry) {
    InputStreamImpl ins;
    shared_ptr<MockDatanode> datanode(new MockDatanode());
    MockDatanodeStub stub;
    LocatedBlock b;
    std::vector<DatanodeInfo> dfv;
    DatanodeInfo df1;
    DatanodeInfo df2;
    dfv.push_back(df1);
    dfv.push_back(df2);
    b.locs = dfv;
    ins.stub = &stub;
    Hdfs::ReplicaNotFoundException e("test", "test", 2, "test");
    EXPECT_CALL(stub, getDatanode()).Times(2).WillOnce(Return(datanode)).WillOnce(Return(datanode));
    EXPECT_CALL(*datanode, getReplicaVisibleLength(_)).Times(2).WillOnce(Throw(e)).WillOnce(
        Return(0));
    EXPECT_EQ(ins.readBlockLength(b), 0);
}

TEST(InputStreamTest, UpdateBlockInfos_LastComplete) {
    MockFileSystemInter * fs = new MockFileSystemInter;
    MockLocatedBlocks * lbs = new MockLocatedBlocks();
    InputStreamImpl ins;
    ins.filesystem = shared_ptr<FileSystemInter>(fs);
    ins.maxGetBlockInfoRetry = 1;
    ins.lastBlockBeingWrittenLength = 1;
    ins.lbs = shared_ptr < MockLocatedBlocks > (lbs);
    EXPECT_CALL(*fs, getBlockLocations(_, _, _, _)).WillOnce(Return());
    EXPECT_CALL(*lbs, isLastBlockComplete()).WillOnce(Return(true));
    EXPECT_NO_THROW(ins.updateBlockInfos());
    EXPECT_EQ(ins.lastBlockBeingWrittenLength, 0);
}

TEST(InputStreamTest, UpdateBlockInfos_NotLastComplete) {
    MockFileSystemInter * fs = new MockFileSystemInter;
    MockLocatedBlocks * lbs = new MockLocatedBlocks();
    InputStreamImpl ins;
    ins.filesystem = shared_ptr<FileSystemInter>(fs);
    ins.maxGetBlockInfoRetry = 1;
    ins.lastBlockBeingWrittenLength = 1;
    ins.lbs = shared_ptr < MockLocatedBlocks > (lbs);
    shared_ptr<MockDatanode> datanode(new MockDatanode());
    MockDatanodeStub stub;
    ins.stub = &stub;
    shared_ptr<LocatedBlock> block(new LocatedBlock);
    std::vector<DatanodeInfo> dfv;
    DatanodeInfo df;
    dfv.push_back(df);
    block->setLocations(dfv);
    EXPECT_CALL(*fs, getBlockLocations(_, _, _, _)).WillOnce(Return());
    EXPECT_CALL(*lbs, isLastBlockComplete()).WillOnce(Return(false));
    EXPECT_CALL(*lbs, getLastBlock()).WillOnce(Return(block));
    EXPECT_CALL(stub, getDatanode()).Times(1).WillOnce(Return(datanode));
    EXPECT_CALL(*datanode, getReplicaVisibleLength(_)).WillOnce(Return(2));
    EXPECT_NO_THROW(ins.updateBlockInfos());
    EXPECT_GT(ins.lastBlockBeingWrittenLength, 0);
}

TEST(InputStreamTest, UpdateBlockInfos_TrowAndTry) {
    MockFileSystemInter * fs = new MockFileSystemInter;
    MockLocatedBlocks * lbs = new MockLocatedBlocks();
    InputStreamImpl ins;
    ins.filesystem = shared_ptr<FileSystemInter>(fs);
    ins.maxGetBlockInfoRetry = 2;
    ins.lastBlockBeingWrittenLength = 1;
    ins.lbs = shared_ptr < MockLocatedBlocks > (lbs);
    Hdfs::HdfsRpcException e("test", "test", 2, "test");
    EXPECT_CALL(*fs, getBlockLocations(_, _, _, _)).Times(2).WillOnce(Throw(e)).WillOnce(
        Throw(e));
    EXPECT_THROW(ins.updateBlockInfos(), Hdfs::HdfsRpcException);
}

TEST(InputStreamTest, ChoseBestNode_Success) {
    InputStreamImpl ins;
    LocatedBlock * lb = new LocatedBlock();
    std::vector<DatanodeInfo> dfv;
    DatanodeInfo df1;
    DatanodeInfo df2;
    dfv.push_back(df1);
    dfv.push_back(df2);
    lb->locs = dfv;
    ins.curBlock = shared_ptr < LocatedBlock > (lb);
    EXPECT_NO_THROW(ins.choseBestNode());
}

TEST(InputStreamTest, SetupBlockReader_Failed) {
    InputStreamImpl ins;
    LocatedBlock * lb = new LocatedBlock();
    std::vector<DatanodeInfo> dfv;
    DatanodeInfo df1;
    DatanodeInfo df2;
    dfv.push_back(df1);
    dfv.push_back(df2);
    lb->locs = dfv;
    ins.curBlock = shared_ptr < LocatedBlock > (lb);
    ins.failedNodes = dfv;
    EXPECT_THROW(ins.setupBlockReader(false), Hdfs::HdfsIOException);
}
