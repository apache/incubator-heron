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
#ifndef _HDFS_LIBHDFS3_SERVER_RPCHELPER_H_
#define _HDFS_LIBHDFS3_SERVER_RPCHELPER_H_

#include <algorithm>
#include <cassert>
#include <functional>
#include <string>
#include <vector>

#include "common/FileStatus.h"
#include "common/Permission.h"
#include "common/Exception.h"
#include "common/ExceptionInternal.h"
#include "common/StackPrinter.h"

#include "proto/ClientDatanodeProtocol.pb.h"
#include "proto/ClientNamenodeProtocol.pb.h"

#include "server/DatanodeInfo.h"
#include "server/ExtendedBlock.h"
#include "server/LocatedBlock.h"
#include "server/LocatedBlocks.h"

namespace Hdfs {
namespace Internal {

static inline void Convert(ExtendedBlock & eb,
                           const ExtendedBlockProto & proto) {
    eb.setBlockId(proto.blockid());
    eb.setGenerationStamp(proto.generationstamp());
    eb.setNumBytes(proto.numbytes());
    eb.setPoolId(proto.poolid());
}

static inline void Convert(Token & token,
                           const TokenProto & proto) {
    token.setIdentifier(proto.identifier());
    token.setKind(proto.kind());
    token.setPassword(proto.password());
    token.setService(proto.service());
}

static inline void Convert(DatanodeInfo & node,
                           const DatanodeInfoProto & proto) {
    const DatanodeIDProto & idProto = proto.id();
    node.setHostName(idProto.hostname());
    node.setInfoPort(idProto.infoport());
    node.setIpAddr(idProto.ipaddr());
    node.setIpcPort(idProto.ipcport());
    node.setDatanodeId(idProto.datanodeuuid());
    node.setXferPort(idProto.xferport());
    node.setLocation(proto.location());
}

static inline shared_ptr<LocatedBlock> Convert(const LocatedBlockProto & proto) {
    Token token;
    shared_ptr<LocatedBlock> lb(new LocatedBlock);
    Convert(token, proto.blocktoken());
    lb->setToken(token);
    std::vector<DatanodeInfo> & nodes = lb->mutableLocations();
    nodes.resize(proto.locs_size());

    for (int i = 0 ; i < proto.locs_size(); ++i) {
        Convert(nodes[i], proto.locs(i));
    }

    if (proto.storagetypes_size() > 0) {
        assert(proto.storagetypes_size() == proto.locs_size());
        std::vector<std::string> & storageIDs = lb->mutableStorageIDs();
        storageIDs.resize(proto.storagetypes_size());

        for (int i = 0; i < proto.storagetypes_size(); ++i) {
            storageIDs[i] = proto.storageids(i);
        }
    }

    Convert(*lb, proto.b());
    lb->setOffset(proto.offset());
    lb->setCorrupt(proto.corrupt());
    return lb;
}

static inline void Convert(LocatedBlocks & lbs,
                           const LocatedBlocksProto & proto) {
    shared_ptr<LocatedBlock> lb;
    lbs.setFileLength(proto.filelength());
    lbs.setIsLastBlockComplete(proto.islastblockcomplete());
    lbs.setUnderConstruction(proto.underconstruction());

    if (proto.has_lastblock()) {
        lb = Convert(proto.lastblock());
        lbs.setLastBlock(lb);
    }

    std::vector<LocatedBlock> & blocks = lbs.getBlocks();
    blocks.resize(proto.blocks_size());

    for (int i = 0; i < proto.blocks_size(); ++i) {
        blocks[i] = *Convert(proto.blocks(i));
    }

    std::sort(blocks.begin(), blocks.end(), std::less<LocatedBlock>());
}

static inline void Convert(const std::string & src, FileStatus & fs,
                           const HdfsFileStatusProto & proto) {
    fs.setAccessTime(proto.access_time());
    fs.setBlocksize(proto.blocksize());
    fs.setGroup(proto.group().c_str());
    fs.setLength(proto.length());
    fs.setModificationTime(proto.modification_time());
    fs.setOwner(proto.owner().c_str());
    fs.setPath((src + "/" + proto.path()).c_str());
    fs.setReplication(proto.block_replication());
    fs.setSymlink(proto.symlink().c_str());
    fs.setPermission(Permission(proto.permission().perm()));
    fs.setIsdir(proto.filetype() == HdfsFileStatusProto::IS_DIR);
}

static inline void Convert(const std::string & src,
                           std::vector<FileStatus> & dl,
                           const DirectoryListingProto & proto) {
    ::google::protobuf::RepeatedPtrField<HdfsFileStatusProto> ptrproto = proto.partiallisting();

    for (int i = 0; i < ptrproto.size(); i++) {
        FileStatus fileStatus;
        Convert(src, fileStatus, ptrproto.Get(i));
        dl.push_back(fileStatus);
    }
}

static inline Token Convert(const TokenProto & proto) {
    Token retval;
    retval.setIdentifier(proto.identifier());
    retval.setKind(proto.kind());
    retval.setPassword(proto.password());
    return retval;
}

/*static inline void Convert(ContentSummary & contentSummary, const ContentSummaryProto & proto) {
    contentSummary.setDirectoryCount(proto.directorycount());
    contentSummary.setFileCount(proto.filecount());
    contentSummary.setLength(proto.length());
    contentSummary.setQuota(proto.quota());
    contentSummary.setSpaceConsumed(proto.spaceconsumed());
    contentSummary.setSpaceQuota(proto.spacequota());
}*/

static inline void Build(const Token & token,
                         TokenProto * proto) {
    proto->set_identifier(token.getIdentifier());
    proto->set_kind(token.getKind());
    proto->set_password(token.getPassword());
    proto->set_service(token.getService());
}

static inline void Build(const Permission & p, FsPermissionProto * proto) {
    proto->set_perm(p.toShort());
}

static inline void Build(const DatanodeInfo & dn, DatanodeIDProto * proto) {
    proto->set_hostname(dn.getHostName());
    proto->set_infoport(dn.getInfoPort());
    proto->set_ipaddr(dn.getIpAddr());
    proto->set_ipcport(dn.getIpcPort());
    proto->set_datanodeuuid(dn.getDatanodeId());
    proto->set_xferport(dn.getXferPort());
}

static inline void Build(const std::vector<DatanodeInfo> & dns,
                         ::google::protobuf::RepeatedPtrField<DatanodeInfoProto> * proto) {
    for (size_t i = 0; i < dns.size(); ++i) {
        DatanodeInfoProto * p = proto->Add();
        Build(dns[i], p->mutable_id());
        p->set_location(dns[i].getLocation());
    }
}

static inline void Build(const ExtendedBlock & eb, ExtendedBlockProto * proto) {
    proto->set_blockid(eb.getBlockId());
    proto->set_generationstamp(eb.getGenerationStamp());
    proto->set_numbytes(eb.getNumBytes());
    proto->set_poolid(eb.getPoolId());
}

static inline void Build(LocatedBlock & b, LocatedBlockProto * proto) {
    proto->set_corrupt(b.isCorrupt());
    proto->set_offset(b.getOffset());
    Build(b, proto->mutable_b());
    Build(b.getLocations(), proto->mutable_locs());
}

/*static inline void Build(const std::vector<LocatedBlock> & blocks,
                         ::google::protobuf::RepeatedPtrField<LocatedBlockProto> * proto) {
    for (size_t i = 0; i < blocks.size(); ++i) {
        LocatedBlockProto * p = proto->Add();
        p->set_corrupt(blocks[i].isCorrupt());
        p->set_offset(blocks[i].getOffset());
        Build(blocks[i], p->mutable_b());
    }
}*/

static inline void Build(const std::vector<std::string> & srcs,
                         ::google::protobuf::RepeatedPtrField<std::string> * proto) {
    for (size_t i = 0; i < srcs.size(); ++i) {
        proto->Add()->assign(srcs[i]);
    }
}

static inline void Build(const std::vector<DatanodeInfo> & dns,
                         ::google::protobuf::RepeatedPtrField<DatanodeIDProto> * proto) {
    for (size_t i = 0; i < dns.size(); ++i) {
        Build(dns[i], proto->Add());
    }
}

}  // namespace Internal
}  // namespace Hdfs

#endif /* _HDFS_LIBHDFS3_SERVER_RPCHELPER_H_ */
