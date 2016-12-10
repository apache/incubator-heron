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

#include "client/FileStatus.h"
#include "client/Permission.h"
#include "ClientDatanodeProtocol.pb.h"
#include "ClientNamenodeProtocol.pb.h"
#include "DatanodeInfo.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "ExtendedBlock.h"
#include "LocatedBlock.h"
#include "LocatedBlocks.h"
#include "StackPrinter.h"

#include <algorithm>
#include <cassert>

using namespace google::protobuf;

namespace Hdfs {
namespace Internal {

class Nothing {
};

template < typename T1 = Nothing, typename T2 = Nothing, typename T3 = Nothing,
         typename T4 = Nothing, typename T5 = Nothing, typename T6 = Nothing,
         typename T7 = Nothing, typename T8 = Nothing, typename T9 = Nothing,
         typename T10 = Nothing, typename T11 = Nothing  >
class UnWrapper: public UnWrapper<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, Nothing> {
private:
    typedef UnWrapper<T2, T3, T4, T5, T6, T7, T8, T9, T10, T11, Nothing> BaseType;

public:
    UnWrapper(const HdfsRpcServerException & e) :
        BaseType(e), e(e) {
    }

    void ATTRIBUTE_NORETURN ATTRIBUTE_NOINLINE unwrap(const char * file,
            int line) {
        if (e.getErrClass() == T1::ReflexName) {
#ifdef NEED_BOOST
            boost::throw_exception(T1(e.getErrMsg(), SkipPathPrefix(file), line, PrintStack(1, STACK_DEPTH).c_str()));
#else
            throw T1(e.getErrMsg(), SkipPathPrefix(file), line, PrintStack(1, STACK_DEPTH).c_str());
#endif
        } else {
            BaseType::unwrap(file, line);
        }
    }
private:
    const HdfsRpcServerException & e;
};

template<>
class UnWrapper < Nothing, Nothing, Nothing, Nothing, Nothing, Nothing, Nothing,
        Nothing, Nothing, Nothing, Nothing > {
public:
    UnWrapper(const HdfsRpcServerException & e) :
        e(e) {
    }
    void ATTRIBUTE_NORETURN ATTRIBUTE_NOINLINE unwrap(const char * file,
            int line) {
        THROW(HdfsIOException,
              "Unexpected exception: when unwrap the rpc remote exception \"%s\", %s in %s: %d",
              e.getErrClass().c_str(), e.getErrMsg().c_str(), file, line);
    }
private:
    const HdfsRpcServerException & e;
};

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
    RepeatedPtrField<HdfsFileStatusProto> ptrproto = proto.partiallisting();

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
                         RepeatedPtrField<DatanodeInfoProto> * proto) {
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
                         RepeatedPtrField<LocatedBlockProto> * proto) {
    for (size_t i = 0; i < blocks.size(); ++i) {
        LocatedBlockProto * p = proto->Add();
        p->set_corrupt(blocks[i].isCorrupt());
        p->set_offset(blocks[i].getOffset());
        Build(blocks[i], p->mutable_b());
    }
}*/

static inline void Build(const std::vector<std::string> & srcs,
                         RepeatedPtrField<std::string> * proto) {
    for (size_t i = 0; i < srcs.size(); ++i) {
        proto->Add()->assign(srcs[i]);
    }
}

static inline void Build(const std::vector<DatanodeInfo> & dns,
                         RepeatedPtrField<DatanodeIDProto> * proto) {
    for (size_t i = 0; i < dns.size(); ++i) {
        Build(dns[i], proto->Add());
    }
}

}
}

#endif /* _HDFS_LIBHDFS3_SERVER_RPCHELPER_H_ */
