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
#ifndef _HDFS_LIBHDFS3_SERVER_READSHORTCIRCUITINFO_H_
#define _HDFS_LIBHDFS3_SERVER_READSHORTCIRCUITINFO_H_

#include "FileWrapper.h"
#include "Hash.h"
#include "LruMap.h"
#include "Memory.h"
#include "network/Socket.h"
#include "rpc/RpcAuth.h"
#include "server/BlockLocalPathInfo.h"
#include "server/DatanodeInfo.h"
#include "server/ExtendedBlock.h"
#include "SessionConfig.h"
#include "Thread.h"
#include "Token.h"

namespace Hdfs {
namespace Internal {

struct ReadShortCircuitInfoKey {
  ReadShortCircuitInfoKey(uint32_t dnPort, int64_t blockId,
                          const std::string& bpid)
      : dnPort(dnPort), blockId(blockId), bpid(bpid) {}

  size_t hash_value() const {
    size_t values[] = {Int32Hasher(dnPort), Int64Hasher(blockId),
                       StringHasher(bpid)};
    return CombineHasher(values, sizeof(values) / sizeof(values[0]));
  }

  bool operator==(const ReadShortCircuitInfoKey& other) const {
    return dnPort == other.dnPort && blockId == other.blockId &&
           bpid == other.bpid;
  }

  uint32_t dnPort;
  int64_t blockId;
  std::string bpid;
};

struct ReadShortCircuitFDHolder {
 public:
  ReadShortCircuitFDHolder() : metafd(-1), datafd(-1) {}
  ~ReadShortCircuitFDHolder();

  int metafd;
  int datafd;
};

class ReadShortCircuitInfo {
 public:
  ReadShortCircuitInfo(const ReadShortCircuitInfoKey& key, bool legacy)
      : legacy(legacy),
        valid(true),
        blockId(key.blockId),
        bpid(key.bpid),
        dnPort(key.dnPort) {}

  ~ReadShortCircuitInfo();

  const shared_ptr<FileWrapper>& getDataFile() const { return dataFile; }

  void setDataFile(shared_ptr<FileWrapper> dataFile) {
    this->dataFile = dataFile;
  }

  const shared_ptr<FileWrapper>& getMetaFile() const { return metaFile; }

  void setMetaFile(shared_ptr<FileWrapper> metaFile) {
    this->metaFile = metaFile;
  }

  bool isValid() const { return valid; }

  void setValid(bool valid) { this->valid = valid; }

  int64_t getBlockId() const { return blockId; }

  void setBlockId(int64_t blockId) { this->blockId = blockId; }

  const std::string& getBpid() const { return bpid; }

  void setBpid(const std::string& bpid) { this->bpid = bpid; }

  uint32_t getDnPort() const { return dnPort; }

  void setDnPort(uint32_t dnPort) { this->dnPort = dnPort; }

  ReadShortCircuitInfoKey getKey() const {
    return ReadShortCircuitInfoKey(dnPort, blockId, bpid);
  }

  bool isLegacy() const { return legacy; }

  void setLegacy(bool legacy) { this->legacy = legacy; }

  const shared_ptr<ReadShortCircuitFDHolder>& getFdHolder() const {
    return fdHolder;
  }

  void setFdHolder(const shared_ptr<ReadShortCircuitFDHolder>& fdHolder) {
    this->fdHolder = fdHolder;
  }

  const std::string formatBlockInfo() const {
    ExtendedBlock block;
    block.setBlockId(blockId);
    block.setPoolId(bpid);
    return block.toString();
  }

 private:
  bool legacy;
  bool valid;
  shared_ptr<FileWrapper> dataFile;
  shared_ptr<FileWrapper> metaFile;
  shared_ptr<ReadShortCircuitFDHolder> fdHolder;
  int64_t blockId;
  std::string bpid;
  uint32_t dnPort;
};

typedef LruMap<ReadShortCircuitInfoKey, shared_ptr<ReadShortCircuitFDHolder> >
    ReadShortCircuitFDCacheType;
typedef LruMap<ReadShortCircuitInfoKey, BlockLocalPathInfo>
    BlockLocalPathInfoCacheType;

class ReadShortCircuitInfoBuilder {
 public:
  ReadShortCircuitInfoBuilder(const DatanodeInfo& dnInfo, const RpcAuth& auth,
                              const SessionConfig& conf);
  shared_ptr<ReadShortCircuitInfo> fetchOrCreate(const ExtendedBlock& block,
                                                 const Token token);
  static void release(const ReadShortCircuitInfo& info);

 private:
  BlockLocalPathInfo getBlockLocalPathInfo(const ExtendedBlock& block,
                                           const Token& token);
  void invalidBlockLocalPathInfo(const ExtendedBlock& block);
  shared_ptr<ReadShortCircuitInfo> createReadShortCircuitInfo(
      const ReadShortCircuitInfoKey& key, const BlockLocalPathInfo& info);
  shared_ptr<ReadShortCircuitInfo> createReadShortCircuitInfo(
      const ReadShortCircuitInfoKey& key, const ExtendedBlock& block,
      const Token& token);
  shared_ptr<ReadShortCircuitInfo> createReadShortCircuitInfo(
      const ReadShortCircuitInfoKey& key,
      const shared_ptr<ReadShortCircuitFDHolder>& fds);
  std::string buildDomainSocketAddress(uint32_t port);
  shared_ptr<Socket> getDomainSocketConnection(const std::string& addr);
  shared_ptr<ReadShortCircuitFDHolder> receiveReadShortCircuitFDs(
      Socket& sock, const ExtendedBlock& block);

 private:
  DatanodeInfo dnInfo;
  RpcAuth auth;
  SessionConfig conf;
  static const int MaxReadShortCircuitVersion = 1;
  static ReadShortCircuitFDCacheType ReadShortCircuitFDCache;
  static BlockLocalPathInfoCacheType BlockLocalPathInfoCache;
};
}
}

HDFS_HASH_DEFINE(::Hdfs::Internal::ReadShortCircuitInfoKey);

#endif /* _HDFS_LIBHDFS3_SERVER_READSHORTCIRCUITINFO_H_ */
