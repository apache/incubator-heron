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
#ifndef _HDFS_LIBHDFS3_MOCK_MOCKNAMENODE_H_
#define _HDFS_LIBHDFS3_MOCK_MOCKNAMENODE_H_

#include "gmock/gmock.h"
#include "server/Namenode.h"
#include "server/LocatedBlocks.h"
#include "server/LocatedBlock.h"
#include "server/ExtendedBlock.h"
#include "server/DatanodeInfo.h"
#include "client/FileStatus.h"

using namespace Hdfs::Internal;
namespace Hdfs {

namespace Mock {

class MockNamenode: public Namenode {
public:
    MOCK_METHOD4(getBlockLocations, void(const std::string & src, int64_t offset,
                            int64_t length, LocatedBlocks & lbs));
    MOCK_METHOD7(create, void(const std::string & src, const Permission & masked,
          const std::string & clientName, int flag, bool createParent,
          short replication, int64_t blockSize));
    MOCK_METHOD2(append, std::pair<shared_ptr<LocatedBlock>,
                 shared_ptr<FileStatus> >(const std::string & src, const std::string & clientName));
    MOCK_METHOD2(setReplication, bool(const std::string & src, short replication));
    MOCK_METHOD2(setPermission, void(const std::string & src,
          const Permission & permission));
    MOCK_METHOD3(setOwner, void(const std::string & src, const std::string & username, const std::string & groupname));
    MOCK_METHOD3(abandonBlock, void(const ExtendedBlock & b, const std::string & src,
          const std::string & holder));
    MOCK_METHOD4(addBlock, shared_ptr<LocatedBlock>(const std::string & src,
          const std::string & clientName, const ExtendedBlock * previous,
          const std::vector<DatanodeInfo> & excludeNodes));
    MOCK_METHOD7(getAdditionalDatanode, shared_ptr<LocatedBlock>(const std::string & src,
             const ExtendedBlock & blk,
             const std::vector<DatanodeInfo> & existings,
             const std::vector<std::string> & storageIDs,
             const std::vector<DatanodeInfo> & excludes, int numAdditionalNodes,
             const std::string & clientName));
    MOCK_METHOD3(complete, bool(const std::string & src,
                        const std::string & clientName, const ExtendedBlock * last));
    MOCK_METHOD1(reportBadBlocks, void(const std::vector<LocatedBlock> & blocks));
    MOCK_METHOD2(concat, void(const std::string & trg,
                       const std::vector<std::string> & srcs));
    MOCK_METHOD3(truncate, bool(const std::string & src, int64_t size,
                        const std::string & clientName));
    MOCK_METHOD2(getLease, void(const std::string & src,
                        const std::string & clientName)) ;
    MOCK_METHOD2(releaseLease, void(const std::string & src,
                          const std::string & clientName));
    MOCK_METHOD2(deleteFile, bool(const std::string & src, bool recursive));
    MOCK_METHOD3(mkdirs, bool(const std::string & src, const Permission & masked,
                     bool createParent)) ;
    MOCK_METHOD1(renewLease, void(const std::string & clientName));
    MOCK_METHOD2(recoverLease, bool(const std::string & src,
                           const std::string & clientName));
    MOCK_METHOD0(getFsStats, std::vector<int64_t>());
    MOCK_METHOD1(metaSave, void(
          const std::string & filename));
    MOCK_METHOD2(getFileInfo, FileStatus(const std::string & src, bool *exist));
    MOCK_METHOD1(getFileLinkInfo, FileStatus(const std::string & src));
    MOCK_METHOD3(setQuota, void(const std::string & path, int64_t namespaceQuota,
                        int64_t diskspaceQuota));
    MOCK_METHOD2(fsync, void(const std::string & src, const std::string & client));
    MOCK_METHOD3(setTimes, void(const std::string & src, int64_t mtime, int64_t atime));
    MOCK_METHOD4(createSymlink, void(const std::string & target,
                             const std::string & link, const Permission & dirPerm,
                             bool createParent));
    MOCK_METHOD1(getLinkTarget, std::string(const std::string & path));
    MOCK_METHOD2(updateBlockForPipeline, shared_ptr<LocatedBlock>(const ExtendedBlock & block,
              const std::string & clientName));
    MOCK_METHOD5(updatePipeline, void(const std::string & clientName,
                              const ExtendedBlock & oldBlock, const ExtendedBlock & newBlock,
                              const std::vector<DatanodeInfo> & newNodes,
                              const std::vector<std::string> & storageIDs));
    MOCK_METHOD4(getListing, bool(const std::string & src,
                           const std::string & startAfter, bool needLocation,
                           std::vector<FileStatus> & dl));
    MOCK_METHOD2(rename, bool(const std::string & src, const std::string & dst));
    MOCK_METHOD1(getDelegationToken, Token(const std::string & renewer) );
    MOCK_METHOD1(renewDelegationToken, int64_t(const Token & token));
    MOCK_METHOD1(cancelDelegationToken, void(const Token & token));
};

}
}

#endif /* _HDFS_LIBHDFS3_MOCK_MOCKNAMENODE_H_ */
