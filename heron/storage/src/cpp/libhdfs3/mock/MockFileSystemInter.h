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
#ifndef _HDFS_LIBHDFS3_MOCK_MOCKFILESYSTEMINTER_H_
#define _HDFS_LIBHDFS3_MOCK_MOCKFILESYSTEMINTER_H_

#include "gmock/gmock.h"

#include "client/DirectoryIterator.h"
#include "client/FileStatus.h"
#include "client/FileSystem.h"
#include "client/FileSystemInter.h"
#include "client/FileSystemKey.h"
#include "client/FileSystemStats.h"
#include "client/Permission.h"
#include "client/UserInfo.h"
#include "Memory.h"
#include "server/DatanodeInfo.h"
#include "server/ExtendedBlock.h"
#include "server/LocatedBlock.h"
#include "server/LocatedBlocks.h"
#include "SessionConfig.h"

#include <string>
#include <vector>

class MockFileSystemInter: public Hdfs::Internal::FileSystemInter {
public:
  MOCK_METHOD0(connect, void());
  MOCK_METHOD0(disconnect, void());
  MOCK_METHOD1(getStandardPath, const std::string(const char * path));
  MOCK_METHOD0(getClientName, const char *());
  MOCK_CONST_METHOD0(getDefaultReplication, int());
  MOCK_CONST_METHOD0(getDefaultBlockSize, int64_t());
  MOCK_CONST_METHOD0(getHomeDirectory, std::string());
  MOCK_METHOD2(deletePath, bool(const char * path, bool recursive));
  MOCK_METHOD2(mkdir, bool(const char * path, const Hdfs::Permission & permission));
  MOCK_METHOD2(mkdirs, bool(const char * path, const Hdfs::Permission & permission));
  MOCK_METHOD1(getFileStatus, Hdfs::FileStatus(const char * path));
  MOCK_METHOD3(setOwner, void(const char * path, const char * username, const char * groupname));
  MOCK_METHOD3(setTimes, void(const char * path, int64_t mtime, int64_t atime));
  MOCK_METHOD2(setPermission, void(const char * path, const Hdfs::Permission &));
  MOCK_METHOD2(setReplication, bool(const char * path, short replication));
  MOCK_METHOD2(rename, bool(const char * src, const char * dst));
  MOCK_METHOD1(setWorkingDirectory, void(const char * path));
  MOCK_CONST_METHOD0(getWorkingDirectory, std::string());
  MOCK_METHOD1(exist, bool(const char * path));
  MOCK_METHOD0(getFsStats, Hdfs::FileSystemStats());
  MOCK_METHOD2(truncate, bool(const char * src, int64_t size));
  MOCK_METHOD1(getDelegationToken, std::string(const char * renewer));
  MOCK_METHOD0(getDelegationToken, std::string());
  MOCK_METHOD1(renewDelegationToken, int64_t(const std::string & token));
  MOCK_METHOD1(cancelDelegationToken, void(const std::string & token));
  MOCK_METHOD6(create, void(const std::string & src, const Hdfs::Permission & masked, int flag, bool createParent, short replication, int64_t blockSize));
  MOCK_METHOD1(append, std::pair<Hdfs::Internal::shared_ptr<Hdfs::Internal::LocatedBlock>,
               Hdfs::Internal::shared_ptr<Hdfs::FileStatus> >(const std::string & src));
  MOCK_METHOD2(abandonBlock, void(const Hdfs::Internal::ExtendedBlock & b, const std::string & srcr));
  MOCK_METHOD3(addBlock, Hdfs::Internal::shared_ptr<Hdfs::Internal::LocatedBlock>(const std::string & src,
          const Hdfs::Internal::ExtendedBlock * previous,
          const std::vector<Hdfs::Internal::DatanodeInfo> & excludeNodes));
  MOCK_METHOD6(getAdditionalDatanode, Hdfs::Internal::shared_ptr<Hdfs::Internal::LocatedBlock> (const std::string & src,
          const Hdfs::Internal::ExtendedBlock & blk,
          const std::vector<Hdfs::Internal::DatanodeInfo> & existings,
          const std::vector<std::string> & storageIDs,
          const std::vector<Hdfs::Internal::DatanodeInfo> & excludes, int numAdditionalNodes));
  MOCK_METHOD2(complete, bool(const std::string & src,
          const Hdfs::Internal::ExtendedBlock * last));
  MOCK_METHOD1(reportBadBlocks, void(const std::vector<Hdfs::Internal::LocatedBlock> & blocks));
  MOCK_METHOD1(fsync, void(const std::string & src));
  MOCK_METHOD1(updateBlockForPipeline, Hdfs::Internal::shared_ptr<Hdfs::Internal::LocatedBlock>(const Hdfs::Internal::ExtendedBlock & block));
  MOCK_METHOD4(updatePipeline, void(const Hdfs::Internal::ExtendedBlock & oldBlock,
          const Hdfs::Internal::ExtendedBlock & newBlock,
          const std::vector<Hdfs::Internal::DatanodeInfo> & newNodes,
          const std::vector<std::string> & storageIDs));
  MOCK_CONST_METHOD0(getConf, const Hdfs::Internal::SessionConfig &());
  MOCK_CONST_METHOD0(getUserInfo, const Hdfs::Internal::UserInfo &());
  MOCK_METHOD4(getBlockLocations, void(const std::string & src, int64_t offset, int64_t length, Hdfs::Internal::LocatedBlocks & lbs));
  MOCK_METHOD4(getListing, bool(const std::string & src, const std::string & , bool needLocation, std::vector<Hdfs::FileStatus> &));
  MOCK_METHOD2(listDirectory, Hdfs::DirectoryIterator(const char *, bool));
  MOCK_METHOD0(renewLease, bool());
  MOCK_METHOD0(registerOpenedOutputStream, void());
  MOCK_METHOD0(unregisterOpenedOutputStream, bool());
  MOCK_METHOD3(getFileBlockLocations, std::vector<Hdfs::BlockLocation> (const char * path, int64_t start, int64_t len));
  MOCK_METHOD2(listAllDirectoryItems, std::vector<Hdfs::FileStatus> (const char * path, bool needLocation));
  MOCK_METHOD0(getPeerCache, Hdfs::Internal::PeerCache &());
};

#endif /* _HDFS_LIBHDFS3_MOCK_MOCKSOCKET_H_ */
