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

#include "client/FileSystem.h"
#include "client/FileSystemImpl.h"
#include "client/FileSystemInter.h"
#include "client/OutputStream.h"
#include "client/OutputStreamImpl.h"
#include "client/Packet.h"
#include "client/Pipeline.h"
#include "DateTime.h"
#include "MockFileSystemInter.h"
#include "MockLeaseRenewer.h"
#include "MockPipeline.h"
#include "NamenodeStub.h"
#include "server/ExtendedBlock.h"
#include "TestDatanodeStub.h"
#include "TestUtil.h"
#include "Thread.h"
#include "XmlConfig.h"

#include <string>

using namespace Hdfs;
using namespace Internal;
using namespace Hdfs::Mock;
using namespace testing;
using ::testing::AtLeast;

#define BASE_DIR "test/"

class TestOutputStream: public ::testing::Test {
public:
    TestOutputStream() {
        renewer = MakeMockLeaseRenewer();
    }

    ~TestOutputStream() {
        ResetMockLeaseRenewer(renewer);
    }

protected:
    shared_ptr<LeaseRenewer> renewer;
    MockFileSystemInter fs;
    OutputStreamImpl ous;
};

class MockPipelineStub: public PipelineStub {
public:
    MOCK_METHOD0(getPipeline, shared_ptr<MockPipeline> ());
};

class MockNamenodeStub: public NamenodeStub {
public:
    MOCK_METHOD0(getNamenode, MockNamenode * ());
};

static void LeaseRenew(int flag) {
    Config conf;
    FileStatus fileinfo;
    fileinfo.setBlocksize(2048);
    fileinfo.setLength(1024);
    shared_ptr<LocatedBlock> lastBlock(new LocatedBlock);
    lastBlock->setNumBytes(0);
    std::pair<shared_ptr<LocatedBlock>, shared_ptr<FileStatus> > lastBlockWithStatus;
    lastBlockWithStatus.first = lastBlock;
    lastBlockWithStatus.second = shared_ptr<FileStatus>(new FileStatus(fileinfo));
    MockNamenodeStub stub;
    SessionConfig sconf(conf);
    shared_ptr<MockFileSystemInter> myfs(new MockFileSystemInter());
    EXPECT_CALL(*myfs, getConf()).Times(1).WillOnce(ReturnRef(sconf));
    //EXPECT_CALL(stub, getNamenode()).Times(1).WillOnce(Return(nn));
    OutputStreamImpl leaseous;

    if (flag & Append) {
        EXPECT_CALL(*myfs, append(_)).Times(1).WillOnce(Return(lastBlockWithStatus));
    } else {
        EXPECT_CALL(*myfs, create(_, _, _, _, _, _)).Times(1);
    }

    EXPECT_CALL(*myfs, complete(_, _)).Times(1).WillOnce(Return(true));
    EXPECT_CALL(GetMockLeaseRenewer(), StartRenew(_)).Times(1);
    EXPECT_CALL(GetMockLeaseRenewer(), StopRenew(_)).Times(1);
    EXPECT_CALL(*myfs, getStandardPath(_)).Times(1);
    EXPECT_NO_THROW(DebugException(leaseous.open(myfs, BASE_DIR"testrenewlease", flag, 0644, true, 0, 2048)));
    EXPECT_NO_THROW(leaseous.close());
};

static void heartBeatSender(int flag) {
    OutputStreamImpl ous;
    shared_ptr<MockPipeline> pipeline(new MockPipeline());
    MockPipelineStub stub;
    ous.stub = &stub;
    MockFileSystemInter * fs = new MockFileSystemInter;
    FileStatus fileinfo;
    fileinfo.setBlocksize(2048);
    fileinfo.setLength(1024);
    Config conf;
    const SessionConfig sessionConf(conf);
    shared_ptr<LocatedBlock> lastBlock(new LocatedBlock);
    lastBlock->setNumBytes(0);
    std::pair<shared_ptr<LocatedBlock>, shared_ptr<FileStatus> > lastBlockWithStatus;
    lastBlockWithStatus.first = lastBlock;
    lastBlockWithStatus.second = shared_ptr<FileStatus>(new FileStatus(fileinfo));
    EXPECT_CALL(*fs, getStandardPath(_)).Times(1).WillOnce(Return("/testheartBeat"));
    EXPECT_CALL(*fs, getConf()).Times(1).WillOnce(ReturnRef(sessionConf));

    if (flag & Append) {
        EXPECT_CALL(*fs, append(_)).Times(1).WillOnce(Return(lastBlockWithStatus));
    } else {
        EXPECT_CALL(*fs, create(_, _, _, _, _, _)).Times(1);
    }

    EXPECT_CALL(*fs, registerOpenedOutputStream()).Times(1);
    EXPECT_NO_THROW(ous.open(shared_ptr<FileSystemInter>(fs), "testheartBeat", flag, 0644, false, 3, 1024 * 1024));
    char buffer[20];
    Hdfs::FillBuffer(buffer, sizeof(buffer), 0);
    EXPECT_NO_THROW(ous.append(buffer, sizeof(buffer)));
    EXPECT_CALL(stub, getPipeline()).Times(1).WillOnce(Return(pipeline));
    EXPECT_CALL(*pipeline, send(_)).Times(3);
    EXPECT_CALL(*pipeline, flush()).Times(1);
    EXPECT_NO_THROW(ous.flush());
    sleep_for(seconds(21));
    EXPECT_CALL(*pipeline, close(_)).Times(1).WillOnce(Return(lastBlock));
    EXPECT_CALL(*fs, fsync(_)).Times(1);
    EXPECT_CALL(*fs, complete(_, _)).Times(1).WillOnce(Return(true));
    EXPECT_CALL(*fs, unregisterOpenedOutputStream()).Times(1);
    EXPECT_NO_THROW(ous.close());
}

static void heartBeatSenderThrow(int flag) {
    OutputStreamImpl ous;
    shared_ptr<MockPipeline> pipeline(new MockPipeline());
    MockPipelineStub stub;
    ous.stub = &stub;
    MockFileSystemInter * fs = new MockFileSystemInter;
    FileStatus fileinfo;
    fileinfo.setBlocksize(2048);
    fileinfo.setLength(1024);
    Config conf;
    const SessionConfig sessionConf(conf);
    shared_ptr<LocatedBlock> lastBlock(new LocatedBlock);
    HdfsIOException e("test", "test", 3, "test");
    lastBlock->setNumBytes(0);
    std::pair<shared_ptr<LocatedBlock>, shared_ptr<FileStatus> > lastBlockWithStatus;
    lastBlockWithStatus.first = lastBlock;
    lastBlockWithStatus.second = shared_ptr<FileStatus>(new FileStatus(fileinfo));
    EXPECT_CALL(*fs, getStandardPath(_)).Times(1).WillOnce(Return("/testheartBeat"));
    EXPECT_CALL(*fs, getConf()).Times(1).WillOnce(ReturnRef(sessionConf));

    if (flag & Append) {
        EXPECT_CALL(*fs, append(_)).Times(1).WillOnce(Return(lastBlockWithStatus));
    } else {
        EXPECT_CALL(*fs, create(_, _, _, _, _, _)).Times(1);
    }

    EXPECT_CALL(*fs, registerOpenedOutputStream()).Times(1);
    EXPECT_NO_THROW(ous.open(shared_ptr<FileSystemInter>(fs), "testheartBeat", flag, 0644, false, 3, 1024 * 1024));
    char buffer[20];
    Hdfs::FillBuffer(buffer, sizeof(buffer), 0);
    EXPECT_NO_THROW(ous.append(buffer, sizeof(buffer)));
    EXPECT_CALL(stub, getPipeline()).Times(1).WillOnce(Return(pipeline));
    EXPECT_CALL(*pipeline, send(_)).Times(2).WillOnce(Return()).WillOnce(Throw(e));
    EXPECT_CALL(*pipeline, flush()).Times(1);
    EXPECT_NO_THROW(ous.flush());
    sleep_for(seconds(11));
    EXPECT_CALL(*pipeline, close(_)).Times(1).WillOnce(Return(lastBlock));
    EXPECT_CALL(*fs, fsync(_)).Times(1);
    EXPECT_CALL(*fs, complete(_, _)).Times(1).WillOnce(Return(true));
    EXPECT_CALL(*fs, unregisterOpenedOutputStream()).Times(1);
    EXPECT_NO_THROW(ous.close());
}

TEST_F(TestOutputStream, LeaseRenewForAppend_Success) {
    LeaseRenew(Create | Append);
}

TEST_F(TestOutputStream, LeaseRenewForCreate_Success) {
    LeaseRenew(Create);
}

TEST_F(TestOutputStream, DISABLED_heartBeatSenderForAppend_Success) {
    heartBeatSender(Create | Append);
}

TEST_F(TestOutputStream, DISABLED_heartBeatSenderForCreate_Success) {
    heartBeatSender(Create);
}

TEST_F(TestOutputStream, DISABLED_heartBeatSenderForCreate_Throw) {
    heartBeatSenderThrow(Create);
}

TEST_F(TestOutputStream, DISABLED_heartBeatSenderForAppend_Throw) {
    heartBeatSenderThrow(Create | Append);
}

TEST_F(TestOutputStream, openForCreate_Success) {
    OutputStreamImpl ous;
    MockFileSystemInter * fs = new MockFileSystemInter;
    Config conf;
    const SessionConfig sessionConf(conf);
    EXPECT_CALL(*fs, getStandardPath(_)).Times(1).WillOnce(Return("/testopen"));
    EXPECT_CALL(*fs, getConf()).Times(1).WillOnce(ReturnRef(sessionConf));
    EXPECT_CALL(*fs, create(_, _, _, _, _, _)).Times(1);
    EXPECT_CALL(GetMockLeaseRenewer(), StartRenew(_)).Times(1);
    EXPECT_CALL(GetMockLeaseRenewer(), StopRenew(_)).Times(1);
    EXPECT_NO_THROW(ous.open(shared_ptr<FileSystemInter>(fs), "testopen", Create, 0644, false, 3, 1024 * 1024));
    EXPECT_CALL(*fs, complete(_, _)).Times(1).WillOnce(Return(true));
    EXPECT_NO_THROW(ous.close());
}

TEST_F(TestOutputStream, registerForCreate_Success) {
    OutputStreamImpl ous;
    MockFileSystemInter * fs = new MockFileSystemInter;
    Config conf;
    const SessionConfig sessionConf(conf);
    EXPECT_CALL(*fs, getStandardPath(_)).Times(1).WillOnce(Return("/testregiester"));
    EXPECT_CALL(*fs, getConf()).Times(1).WillOnce(ReturnRef(sessionConf));
    EXPECT_CALL(*fs, create(_, _, _, _, _, _)).Times(1);
    EXPECT_CALL(GetMockLeaseRenewer(), StartRenew(_)).Times(1);
    EXPECT_CALL(GetMockLeaseRenewer(), StopRenew(_)).Times(1);
    EXPECT_NO_THROW(ous.open(shared_ptr<FileSystemInter>(fs), "testregiester", Create, 0644, false, 3, 1024 * 1024));
    EXPECT_CALL(*fs, complete(_, _)).Times(1).WillOnce(Return(true));
    EXPECT_NO_THROW(ous.close());
}

TEST_F(TestOutputStream, registerForAppend_Success) {
    OutputStreamImpl ous;
    MockFileSystemInter * fs = new MockFileSystemInter;
    Config conf;
    const SessionConfig sessionConf(conf);
    FileStatus fileinfo;
    fileinfo.setBlocksize(2048);
    fileinfo.setLength(1024);
    shared_ptr<LocatedBlock> lastBlock(new LocatedBlock);
    lastBlock->setNumBytes(0);
    std::pair<shared_ptr<LocatedBlock>, shared_ptr<FileStatus> > lastBlockWithStatus;
    lastBlockWithStatus.first = lastBlock;
    lastBlockWithStatus.second = shared_ptr<FileStatus>(new FileStatus(fileinfo));
    EXPECT_CALL(*fs, getStandardPath(_)).Times(1).WillOnce(Return("/testregiester"));
    EXPECT_CALL(*fs, getConf()).Times(1).WillOnce(ReturnRef(sessionConf));
    EXPECT_CALL(*fs, append(_)).Times(1).WillOnce(Return(lastBlockWithStatus));
    EXPECT_CALL(GetMockLeaseRenewer(), StartRenew(_)).Times(1);
    EXPECT_CALL(GetMockLeaseRenewer(), StopRenew(_)).Times(1);
    EXPECT_NO_THROW(ous.open(shared_ptr<FileSystemInter>(fs), "testregiester", Append, 0644, false, 0, 0));
    EXPECT_CALL(*fs, complete(_, _)).Times(1).WillOnce(Return(true));
    EXPECT_NO_THROW(ous.close());
}

TEST_F(TestOutputStream, openForCreate_Fail) {
    OutputStreamImpl ous;
    MockFileSystemInter * fs = new MockFileSystemInter;
    Config conf;
    const SessionConfig sessionConf(conf);
    HdfsIOException e("test", "test", 2, "test");
    EXPECT_CALL(*fs, getStandardPath(_)).Times(1).WillOnce(Return("/testopen"));
    EXPECT_CALL(*fs, getConf()).Times(1).WillOnce(ReturnRef(sessionConf));
    EXPECT_CALL(*fs, create(_, _, _, _, _, _)).Times(1).WillOnce(Throw(e));
    EXPECT_THROW(ous.open(shared_ptr<FileSystemInter>(fs), "testopen", Create, 0644, false, 3, 1024 * 1024), HdfsIOException);
}


TEST_F(TestOutputStream, openForAppend_Success) {
    OutputStreamImpl ous;
    MockFileSystemInter * fs = new MockFileSystemInter;
    Config conf;
    const SessionConfig sessionConf(conf);
    FileStatus fileinfo;
    fileinfo.setBlocksize(2048);
    fileinfo.setLength(1024);
    shared_ptr<LocatedBlock> lastBlock(new LocatedBlock);
    lastBlock->setNumBytes(0);
    std::pair<shared_ptr<LocatedBlock>, shared_ptr<FileStatus> > lastBlockWithStatus;
    lastBlockWithStatus.first = lastBlock;
    lastBlockWithStatus.second = shared_ptr<FileStatus>(new FileStatus(fileinfo));
    EXPECT_CALL(*fs, getStandardPath(_)).Times(1).WillOnce(Return("/testopen"));
    EXPECT_CALL(*fs, getConf()).Times(1).WillOnce(ReturnRef(sessionConf));
    EXPECT_CALL(*fs, append(_)).Times(1).WillOnce(Return(lastBlockWithStatus));
    EXPECT_CALL(GetMockLeaseRenewer(), StartRenew(_)).Times(1);
    EXPECT_CALL(GetMockLeaseRenewer(), StopRenew(_)).Times(1);
    EXPECT_NO_THROW(ous.open(shared_ptr<FileSystemInter>(fs), "testopen", Append, 0644, false, 0, 0));
    EXPECT_CALL(*fs, complete(_, _)).Times(1).WillOnce(Return(true));
    EXPECT_NO_THROW(ous.close());
}

TEST_F(TestOutputStream, openForAppend_Fail) {
    OutputStreamImpl ous;
    MockFileSystemInter * fs = new MockFileSystemInter;
    Config conf;
    const SessionConfig sessionConf(conf);
    FileStatus fileinfo;
    fileinfo.setBlocksize(2048);
    fileinfo.setLength(1024);
    EXPECT_CALL(*fs, getStandardPath(_)).Times(1).WillOnce(Return("/testopen"));
    EXPECT_CALL(*fs, getConf()).Times(1).WillOnce(ReturnRef(sessionConf));
    EXPECT_CALL(*fs, append(_)).Times(1).WillOnce(Throw(FileNotFoundException("test", "test", 2, "test")));
    EXPECT_THROW(ous.open(shared_ptr<FileSystemInter>(fs), "testopen", Append, 0644, false, 0, 0), FileNotFoundException);
}

TEST_F(TestOutputStream, append_Success) {
    OutputStreamImpl ous;
    shared_ptr<MockPipeline> pipelineStub(new MockPipeline());
    MockPipelineStub stub;
    ous.stub = &stub;
    MockFileSystemInter * fs = new MockFileSystemInter;
    FileStatus fileinfo;
    fileinfo.setBlocksize(2048);
    fileinfo.setLength(1024);
    Config conf;
    const SessionConfig sessionConf(conf);
    shared_ptr<LocatedBlock> lastBlock(new LocatedBlock);
    lastBlock->setNumBytes(0);
    std::pair<shared_ptr<LocatedBlock>, shared_ptr<FileStatus> > lastBlockWithStatus;
    lastBlockWithStatus.first = lastBlock;
    lastBlockWithStatus.second = shared_ptr<FileStatus>(new FileStatus(fileinfo));
    EXPECT_CALL(*fs, getStandardPath(_)).Times(1).WillOnce(Return("/testopen"));
    EXPECT_CALL(*fs, getConf()).Times(1).WillOnce(ReturnRef(sessionConf));
    EXPECT_CALL(*fs, append(_)).Times(1).WillOnce(Return(lastBlockWithStatus));
    EXPECT_CALL(GetMockLeaseRenewer(), StartRenew(_)).Times(1);
    EXPECT_CALL(GetMockLeaseRenewer(), StopRenew(_)).Times(1);
    EXPECT_NO_THROW(ous.open(shared_ptr<FileSystemInter>(fs), "testopen", Create | Append, 0644, false, 3, 2048));
    char buffer[4096 + 523];
    Hdfs::FillBuffer(buffer, sizeof(buffer), 0);
    EXPECT_CALL(stub, getPipeline()).Times(3).WillOnce(Return(pipelineStub)).WillOnce(Return(pipelineStub)).WillOnce(Return(pipelineStub));
    EXPECT_CALL(*pipelineStub, send(_)).Times(4);
    EXPECT_CALL(*pipelineStub, close(_)).Times(2).WillOnce(Return(lastBlock)).WillOnce(Return(lastBlock));
    EXPECT_CALL(*fs, fsync(_)).Times(2);
    EXPECT_NO_THROW(ous.append(buffer, sizeof(buffer)));
    EXPECT_CALL(*pipelineStub, close(_)).Times(1).WillOnce(Return(lastBlock));
    EXPECT_CALL(*fs, fsync(_)).Times(1);
    EXPECT_CALL(*fs, complete(_, _)).Times(1).WillOnce(Return(true));
    EXPECT_NO_THROW(ous.close());
}

TEST_F(TestOutputStream, flush_Success) {
    OutputStreamImpl ous;
    shared_ptr<MockPipeline> pipelineStub(new MockPipeline());
    MockPipelineStub stub;
    ous.stub = &stub;
    MockFileSystemInter * fs = new MockFileSystemInter;
    FileStatus fileinfo;
    fileinfo.setBlocksize(2048);
    fileinfo.setLength(1024);
    Config conf;
    const SessionConfig sessionConf(conf);
    shared_ptr<LocatedBlock> lastBlock(new LocatedBlock);
    HdfsIOException e("test", "test", 3, "test");
    lastBlock->setNumBytes(0);
    std::pair<shared_ptr<LocatedBlock>, shared_ptr<FileStatus> > lastBlockWithStatus;
    lastBlockWithStatus.first = lastBlock;
    lastBlockWithStatus.second = shared_ptr<FileStatus>(new FileStatus(fileinfo));
    EXPECT_CALL(*fs, getStandardPath(_)).Times(1).WillOnce(Return("/testflush"));
    EXPECT_CALL(*fs, getConf()).Times(1).WillOnce(ReturnRef(sessionConf));
    EXPECT_CALL(*fs, append(_)).Times(1).WillOnce(Return(lastBlockWithStatus));
    EXPECT_CALL(GetMockLeaseRenewer(), StartRenew(_)).Times(1);
    EXPECT_CALL(GetMockLeaseRenewer(), StopRenew(_)).Times(1);
    EXPECT_NO_THROW(ous.open(shared_ptr<FileSystemInter>(fs), "testflush", Create | Append, 0644, false, 3, 1024 * 1024));
    char buffer[20];
    Hdfs::FillBuffer(buffer, sizeof(buffer), 0);
    EXPECT_NO_THROW(ous.append(buffer, sizeof(buffer)));
    EXPECT_CALL(stub, getPipeline()).Times(1).WillOnce(Return(pipelineStub));
    EXPECT_CALL(*pipelineStub, send(_)).Times(1);
    EXPECT_CALL(*pipelineStub, flush()).Times(1);
    EXPECT_NO_THROW(ous.flush());
    EXPECT_CALL(*pipelineStub, close(_)).Times(1).WillOnce(Return(lastBlock));
    EXPECT_CALL(*fs, fsync(_)).Times(1);
    EXPECT_CALL(*fs, complete(_, _)).Times(1).WillOnce(Return(true));
    EXPECT_NO_THROW(ous.close());
}

TEST_F(TestOutputStream, ValidateFirstBadLink) {
    EXPECT_NO_THROW(PipelineImpl::checkBadLinkFormat(""));
    EXPECT_NO_THROW(PipelineImpl::checkBadLinkFormat("8.8.8.8:1234"));
    EXPECT_NO_THROW(PipelineImpl::checkBadLinkFormat("2001:0db8:85a3:0000:0000:8a2e:0370:7334:50010"));
    EXPECT_THROW(PipelineImpl::checkBadLinkFormat("3"), HdfsException);
    EXPECT_THROW(PipelineImpl::checkBadLinkFormat("8.8.8.8"), HdfsException);
    EXPECT_THROW(PipelineImpl::checkBadLinkFormat("8.8.8.8:"), HdfsException);
    EXPECT_THROW(PipelineImpl::checkBadLinkFormat("8.8.8.888:50010"), HdfsException);
    EXPECT_THROW(PipelineImpl::checkBadLinkFormat("8.8.8.8:500101"), HdfsException);
    EXPECT_THROW(PipelineImpl::checkBadLinkFormat("8.8.8.8:50010a"), HdfsException);
}

