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
#include "client/DataTransferProtocolSender.h"
#include "ReadShortCircuitInfo.h"
#include "server/Datanode.h"
#include "datatransfer.pb.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "network/DomainSocket.h"
#include "SWCrc32c.h"
#include "HWCrc32c.h"
#include "StringUtil.h"

#include <inttypes.h>
#include <sstream>
#include <vector>

namespace Hdfs {
namespace Internal {

ReadShortCircuitFDCacheType
    ReadShortCircuitInfoBuilder::ReadShortCircuitFDCache;
BlockLocalPathInfoCacheType
    ReadShortCircuitInfoBuilder::BlockLocalPathInfoCache;

ReadShortCircuitInfo::~ReadShortCircuitInfo() {
  try {
    dataFile.reset();
    metaFile.reset();
    ReadShortCircuitInfoBuilder::release(*this);
  } catch (...) {
  }
}

ReadShortCircuitFDHolder::~ReadShortCircuitFDHolder() {
  if (metafd != -1) {
    ::close(metafd);
  }

  if (datafd != -1) {
    ::close(datafd);
  }
}

ReadShortCircuitInfoBuilder::ReadShortCircuitInfoBuilder(
    const DatanodeInfo& dnInfo, const RpcAuth& auth, const SessionConfig& conf)
    : dnInfo(dnInfo), auth(auth), conf(conf) {}

shared_ptr<ReadShortCircuitInfo> ReadShortCircuitInfoBuilder::fetchOrCreate(
    const ExtendedBlock& block, const Token token) {
  shared_ptr<ReadShortCircuitInfo> retval;
  ReadShortCircuitInfoKey key(dnInfo.getXferPort(), block.getBlockId(),
                              block.getPoolId());

  if (conf.isLegacyLocalBlockReader()) {
    if (auth.getProtocol() != AuthProtocol::NONE) {
      LOG(WARNING,
          "Legacy read-shortcircuit only works for simple "
          "authentication");
      return shared_ptr<ReadShortCircuitInfo>();
    }

    BlockLocalPathInfo info = getBlockLocalPathInfo(block, token);
    assert(block.getBlockId() == info.getBlock().getBlockId() &&
           block.getPoolId() == info.getBlock().getPoolId());

    if (0 != access(info.getLocalMetaPath(), R_OK)) {
      invalidBlockLocalPathInfo(block);
      LOG(WARNING,
          "Legacy read-shortcircuit is enabled but path:%s is not "
          "readable.",
          info.getLocalMetaPath());
      return shared_ptr<ReadShortCircuitInfo>();
    }

    retval = createReadShortCircuitInfo(key, info);
  } else {
    shared_ptr<ReadShortCircuitFDHolder> fds;

    // find a pair available file descriptors in cache.
    if (ReadShortCircuitFDCache.findAndErase(key, &fds)) {
      try {
        LOG(DEBUG1,
            "Get file descriptors from cache for block %s, cache size %zu",
            block.toString().c_str(), ReadShortCircuitFDCache.size());

        return createReadShortCircuitInfo(key, fds);
      } catch (...) {
        // failed to create file wrapper from fds, retry with new fds.
      }
    }

    // create a new one
    retval = createReadShortCircuitInfo(key, block, token);
    ReadShortCircuitFDCache.setMaxSize(conf.getMaxFileDescriptorCacheSize());
  }

  return retval;
}

void ReadShortCircuitInfoBuilder::release(const ReadShortCircuitInfo& info) {
  if (info.isValid() && !info.isLegacy()) {
    ReadShortCircuitFDCache.insert(info.getKey(), info.getFdHolder());
    LOG(DEBUG1,
        "Inserted file descriptors into cache for block %s, cache size %zu",
        info.formatBlockInfo().c_str(), ReadShortCircuitFDCache.size());
  }
}

BlockLocalPathInfo ReadShortCircuitInfoBuilder::getBlockLocalPathInfo(
    const ExtendedBlock& block, const Token& token) {
  BlockLocalPathInfo retval;

  ReadShortCircuitInfoKey key(dnInfo.getXferPort(), block.getBlockId(),
                              block.getPoolId());

  try {
    if (!BlockLocalPathInfoCache.find(key, &retval)) {
      RpcAuth a = auth;
      SessionConfig c = conf;
      c.setRpcMaxRetryOnConnect(1);

      /*
       * only kerberos based authentication is allowed, do not add
       * token
       */
      shared_ptr<Datanode> dn = shared_ptr<Datanode>(new DatanodeImpl(
          dnInfo.getIpAddr().c_str(), dnInfo.getIpcPort(), c, a));
      dn->getBlockLocalPathInfo(block, token, retval);

      BlockLocalPathInfoCache.setMaxSize(conf.getMaxLocalBlockInfoCacheSize());
      BlockLocalPathInfoCache.insert(key, retval);

      LOG(DEBUG1, "Inserted block %s to local block info cache, cache size %zu",
          block.toString().c_str(), BlockLocalPathInfoCache.size());
    } else {
      LOG(DEBUG1,
          "Get local block info from cache for block %s, cache size %zu",
          block.toString().c_str(), BlockLocalPathInfoCache.size());
    }
  } catch (const HdfsIOException& e) {
    throw;
  } catch (const HdfsException& e) {
    NESTED_THROW(HdfsIOException,
                 "ReadShortCircuitInfoBuilder: Failed to get block local "
                 "path information.");
  }

  return retval;
}

void ReadShortCircuitInfoBuilder::invalidBlockLocalPathInfo(
    const ExtendedBlock& block) {
  BlockLocalPathInfoCache.erase(ReadShortCircuitInfoKey(
      dnInfo.getXferPort(), block.getBlockId(), block.getPoolId()));
}

shared_ptr<ReadShortCircuitInfo>
ReadShortCircuitInfoBuilder::createReadShortCircuitInfo(
    const ReadShortCircuitInfoKey& key, const BlockLocalPathInfo& info) {
  shared_ptr<FileWrapper> dataFile;
  shared_ptr<FileWrapper> metaFile;

  std::string metaFilePath = info.getLocalMetaPath();
  std::string dataFilePath = info.getLocalBlockPath();

  if (conf.doUseMappedFile()) {
    metaFile = shared_ptr<MappedFileWrapper>(new MappedFileWrapper);
    dataFile = shared_ptr<MappedFileWrapper>(new MappedFileWrapper);
  } else {
    metaFile = shared_ptr<CFileWrapper>(new CFileWrapper);
    dataFile = shared_ptr<CFileWrapper>(new CFileWrapper);
  }

  if (!metaFile->open(metaFilePath)) {
    THROW(HdfsIOException,
          "ReadShortCircuitInfoBuilder cannot open metadata file \"%s\", %s",
          metaFilePath.c_str(), GetSystemErrorInfo(errno));
  }

  if (!dataFile->open(dataFilePath)) {
    THROW(HdfsIOException,
          "ReadShortCircuitInfoBuilder cannot open data file \"%s\", %s",
          dataFilePath.c_str(), GetSystemErrorInfo(errno));
  }

  dataFile->seek(0);
  metaFile->seek(0);

  shared_ptr<ReadShortCircuitInfo> retval(new ReadShortCircuitInfo(key, true));
  retval->setDataFile(dataFile);
  retval->setMetaFile(metaFile);
  return retval;
}

std::string ReadShortCircuitInfoBuilder::buildDomainSocketAddress(
    uint32_t port) {
  std::string domainSocketPath = conf.getDomainSocketPath();

  if (domainSocketPath.empty()) {
    THROW(HdfsIOException,
          "ReadShortCircuitInfoBuilder: \"dfs.domain.socket.path\" is not "
          "set");
  }

  std::stringstream ss;
  ss.imbue(std::locale::classic());
  ss << port;
  StringReplaceAll(domainSocketPath, "_PORT", ss.str());

  return domainSocketPath;
}

shared_ptr<ReadShortCircuitInfo>
ReadShortCircuitInfoBuilder::createReadShortCircuitInfo(
    const ReadShortCircuitInfoKey& key, const ExtendedBlock& block,
    const Token& token) {
  std::string addr = buildDomainSocketAddress(key.dnPort);
  DomainSocketImpl sock;
  sock.connect(addr.c_str(), 0, conf.getInputConnTimeout());
  DataTransferProtocolSender sender(sock, conf.getInputWriteTimeout(), addr);
  sender.requestShortCircuitFds(block, token, MaxReadShortCircuitVersion);
  shared_ptr<ReadShortCircuitFDHolder> fds =
      receiveReadShortCircuitFDs(sock, block);
  return createReadShortCircuitInfo(key, fds);
}

shared_ptr<ReadShortCircuitFDHolder>
ReadShortCircuitInfoBuilder::receiveReadShortCircuitFDs(
    Socket& sock, const ExtendedBlock& block) {
  std::vector<char> respBuffer;
  int readTimeout = conf.getInputReadTimeout();
  shared_ptr<BufferedSocketReader> in(
      new BufferedSocketReaderImpl(sock, 0));  // disable buffer
  int32_t respSize = in->readVarint32(readTimeout);

  if (respSize <= 0 || respSize > 10 * 1024 * 1024) {
    THROW(HdfsIOException,
          "ReadShortCircuitInfoBuilder get a invalid response size: %d, "
          "Block: %s, "
          "from Datanode: %s",
          respSize, block.toString().c_str(), dnInfo.formatAddress().c_str());
  }

  respBuffer.resize(respSize);
  in->readFully(&respBuffer[0], respSize, readTimeout);
  BlockOpResponseProto resp;

  if (!resp.ParseFromArray(&respBuffer[0], respBuffer.size())) {
    THROW(HdfsIOException,
          "ReadShortCircuitInfoBuilder cannot parse BlockOpResponseProto "
          "from "
          "Datanode response, "
          "Block: %s, from Datanode: %s",
          block.toString().c_str(), dnInfo.formatAddress().c_str());
  }

  if (resp.status() != Status::DT_PROTO_SUCCESS) {
    std::string msg;

    if (resp.has_message()) {
      msg = resp.message();
    }

    if (resp.status() == Status::DT_PROTO_ERROR_ACCESS_TOKEN) {
      THROW(HdfsInvalidBlockToken,
            "ReadShortCircuitInfoBuilder: block's token is invalid. "
            "Datanode: %s, Block: %s",
            dnInfo.formatAddress().c_str(), block.toString().c_str());
    } else if (resp.status() == Status::DT_PROTO_ERROR_UNSUPPORTED) {
      THROW(HdfsIOException,
            "short-circuit read access is disabled for "
            "DataNode %s. reason: %s",
            dnInfo.formatAddress().c_str(),
            (msg.empty() ? "check Datanode's log for more information"
                         : msg.c_str()));
    } else {
      THROW(HdfsIOException,
            "ReadShortCircuitInfoBuilder: Datanode return an error when "
            "sending read request to Datanode: %s, Block: %s, %s.",
            dnInfo.formatAddress().c_str(), block.toString().c_str(),
            (msg.empty() ? "check Datanode's log for more information"
                         : msg.c_str()));
    }
  }

  DomainSocketImpl* domainSocket = dynamic_cast<DomainSocketImpl*>(&sock);

  if (NULL == domainSocket) {
    THROW(HdfsIOException, "Read short-circuit only works with Domain Socket");
  }

  shared_ptr<ReadShortCircuitFDHolder> fds(new ReadShortCircuitFDHolder);

  std::vector<int> tempFds(2, -1);
  respBuffer.resize(1);
  domainSocket->receiveFileDescriptors(&tempFds[0], tempFds.size(),
                                       &respBuffer[0], respBuffer.size());

  assert(tempFds[0] != -1 && "failed to receive data file descriptor");
  assert(tempFds[1] != -1 && "failed to receive metadata file descriptor");

  fds->datafd = tempFds[0];
  fds->metafd = tempFds[1];

  return fds;
}

shared_ptr<ReadShortCircuitInfo>
ReadShortCircuitInfoBuilder::createReadShortCircuitInfo(
    const ReadShortCircuitInfoKey& key,
    const shared_ptr<ReadShortCircuitFDHolder>& fds) {
  shared_ptr<FileWrapper> dataFile;
  shared_ptr<FileWrapper> metaFile;

  if (conf.doUseMappedFile()) {
    metaFile = shared_ptr<MappedFileWrapper>(new MappedFileWrapper);
    dataFile = shared_ptr<MappedFileWrapper>(new MappedFileWrapper);
  } else {
    metaFile = shared_ptr<CFileWrapper>(new CFileWrapper);
    dataFile = shared_ptr<CFileWrapper>(new CFileWrapper);
  }

  metaFile->open(fds->metafd, false);
  dataFile->open(fds->datafd, false);

  dataFile->seek(0);
  metaFile->seek(0);

  shared_ptr<ReadShortCircuitInfo> retval(new ReadShortCircuitInfo(key, false));

  retval->setFdHolder(fds);
  retval->setDataFile(dataFile);
  retval->setMetaFile(metaFile);

  return retval;
}
}
}
