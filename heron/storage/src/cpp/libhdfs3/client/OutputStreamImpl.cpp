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
#include "Atomic.h"
#include "DateTime.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "FileSystemInter.h"
#include "HWCrc32c.h"
#include "LeaseRenewer.h"
#include "Logger.h"
#include "OutputStream.h"
#include "OutputStreamImpl.h"
#include "Packet.h"
#include "PacketHeader.h"
#include "SWCrc32c.h"

#include <cassert>
#include <inttypes.h>

namespace Hdfs {
namespace Internal {

OutputStreamImpl::OutputStreamImpl() :
/*heartBeatStop(true),*/ closed(true), isAppend(false), syncBlock(false), checksumSize(0), chunkSize(
        0), chunksPerPacket(0), closeTimeout(0), heartBeatInterval(0), packetSize(0), position(
            0), replication(0), blockSize(0), bytesWritten(0), cursor(0), lastFlushed(
                0), nextSeqNo(0), packets(0) {
    if (HWCrc32c::available()) {
        checksum = shared_ptr < Checksum > (new HWCrc32c());
    } else {
        checksum = shared_ptr < Checksum > (new SWCrc32c());
    }

    checksumSize = sizeof(int32_t);
    lastSend = steady_clock::now();
#ifdef MOCK
    stub = NULL;
#endif
}

OutputStreamImpl::~OutputStreamImpl() {
    if (!closed) {
        try {
            close();
        } catch (...) {
        }
    }
}

void OutputStreamImpl::checkStatus() {
    if (closed) {
        THROW(HdfsIOException, "OutputStreamImpl: stream is not opened.");
    }

    lock_guard < mutex > lock(mut);

    if (lastError != exception_ptr()) {
        rethrow_exception(lastError);
    }
}

void OutputStreamImpl::setError(const exception_ptr & error) {
    try {
        lock_guard < mutex > lock(mut);
        lastError = error;
    } catch (...) {
    }
}

/**
 * To create or append a file.
 * @param fs hdfs file system.
 * @param path the file path.
 * @param flag creation flag, can be Create, Append or Create|Overwrite.
 * @param permission create a new file with given permission.
 * @param createParent if the parent does not exist, create it.
 * @param replication create a file with given number of replication.
 * @param blockSize  create a file with given block size.
 */
void OutputStreamImpl::open(shared_ptr<FileSystemInter> fs, const char * path, int flag,
                            const Permission & permission, bool createParent, int replication,
                            int64_t blockSize) {
    if (NULL == path || 0 == strlen(path) || replication < 0 || blockSize < 0) {
        THROW(InvalidParameter, "Invalid parameter.");
    }

    if (!(flag == Create || flag == (Create | SyncBlock) || flag == Overwrite
            || flag == (Overwrite | SyncBlock) || flag == Append
            || flag == (Append | SyncBlock) || flag == (Create | Overwrite)
            || flag == (Create | Overwrite | SyncBlock)
            || flag == (Create | Append)
            || flag == (Create | Append | SyncBlock))) {
        THROW(InvalidParameter, "Invalid flag.");
    }

    try {
        openInternal(fs, path, flag, permission, createParent, replication,
                     blockSize);
    } catch (...) {
        reset();
        throw;
    }
}

void OutputStreamImpl::computePacketChunkSize() {
    int chunkSizeWithChecksum = chunkSize + checksumSize;
    static const int packetHeaderSize = PacketHeader::GetPkgHeaderSize();
    chunksPerPacket =
        (packetSize - packetHeaderSize + chunkSizeWithChecksum - 1)
        / chunkSizeWithChecksum;
    chunksPerPacket = chunksPerPacket > 1 ? chunksPerPacket : 1;
    packetSize = chunksPerPacket * chunkSizeWithChecksum + packetHeaderSize;
    buffer.resize(chunkSize);
}

void OutputStreamImpl::initAppend() {
    FileStatus fileInfo;
    std::pair<shared_ptr<LocatedBlock>, shared_ptr<FileStatus> > lastBlockWithStatus;
    lastBlockWithStatus = filesystem->append(this->path);
    lastBlock = lastBlockWithStatus.first;

    if (lastBlockWithStatus.second) {
        fileInfo = *lastBlockWithStatus.second;
    } else {
        fileInfo = filesystem->getFileStatus(this->path.c_str());
    }

    closed = false;

    try {
        this->blockSize = fileInfo.getBlockSize();
        cursor = fileInfo.getLength();

        if (lastBlock) {
            isAppend = true;
            bytesWritten = lastBlock->getNumBytes();
            int64_t usedInLastBlock = fileInfo.getLength() % blockSize;
            int64_t freeInLastBlock = blockSize - usedInLastBlock;

            if (freeInLastBlock == this->blockSize) {
                THROW(HdfsIOException,
                      "OutputStreamImpl: the last block for file %s is full.",
                      this->path.c_str());
            }

            int usedInCksum = cursor % chunkSize;
            int freeInCksum = chunkSize - usedInCksum;

            if (usedInCksum > 0 && freeInCksum > 0) {
                /*
                 * if there is space in the last partial chunk, then
                 * setup in such a way that the next packet will have only
                 * one chunk that fills up the partial chunk.
                 */
                packetSize = 0;
                chunkSize = freeInCksum;
            } else {
                /*
                 * if the remaining space in the block is smaller than
                 * that expected size of of a packet, then create
                 * smaller size packet.
                 */
                packetSize =
                    packetSize < freeInLastBlock ?
                    packetSize : static_cast<int>(freeInLastBlock);
            }
        }
    } catch (...) {
        completeFile(false);
        reset();
        throw;
    }

    computePacketChunkSize();
}

void OutputStreamImpl::openInternal(shared_ptr<FileSystemInter> fs, const char * path,
                                    int flag, const Permission & permission, bool createParent,
                                    int replication, int64_t blockSize) {
    filesystem = fs;
    this->path = fs->getStandardPath(path);
    this->replication = replication;
    this->blockSize = blockSize;
    syncBlock = flag & SyncBlock;
    conf = shared_ptr < SessionConfig > (new SessionConfig(fs->getConf()));
    LOG(DEBUG2, "open file %s for %s", this->path.c_str(), (flag & Append ? "append" : "write"));
    packets.setMaxSize(conf->getPacketPoolSize());

    if (0 == replication) {
        this->replication = conf->getDefaultReplica();
    } else {
        this->replication = replication;
    }

    if (0 == blockSize) {
        this->blockSize = conf->getDefaultBlockSize();
    } else {
        this->blockSize = blockSize;
    }

    chunkSize = conf->getDefaultChunkSize();
    packetSize = conf->getDefaultPacketSize();
    heartBeatInterval = conf->getHeartBeatInterval();
    closeTimeout = conf->getCloseFileTimeout();

    if (packetSize < chunkSize) {
        THROW(InvalidParameter,
              "OutputStreamImpl: packet size %d is less than the chunk size %d.",
              packetSize, chunkSize);
    }

    if (0 != this->blockSize % chunkSize) {
        THROW(InvalidParameter,
              "OutputStreamImpl: block size %" PRId64 " is not the multiply of chunk size %d.",
              this->blockSize, chunkSize);
    }

    try {
        if (flag & Append) {
            initAppend();
            LeaseRenewer::GetLeaseRenewer().StartRenew(filesystem);
            return;
        }
    } catch (const FileNotFoundException & e) {
        if (!(flag & Create)) {
            throw;
        }
    }

    assert((flag & Create) || (flag & Overwrite));
    fs->create(this->path, permission, flag, createParent, this->replication,
               this->blockSize);
    closed = false;
    computePacketChunkSize();
    LeaseRenewer::GetLeaseRenewer().StartRenew(filesystem);
}

/**
 * To append data to file.
 * @param buf the data used to append.
 * @param size the data size.
 */
void OutputStreamImpl::append(const char * buf, int64_t size) {
    LOG(DEBUG3, "append file %s size is %" PRId64 ", offset %" PRId64 " next pos %" PRId64, path.c_str(), size, cursor, size + cursor);

    if (NULL == buf || size < 0) {
        THROW(InvalidParameter, "Invalid parameter.");
    }

    checkStatus();

    try {
        appendInternal(buf, size);
    } catch (...) {
        setError(current_exception());
        throw;
    }
}

void OutputStreamImpl::appendInternal(const char * buf, int64_t size) {
    int64_t todo = size;

    while (todo > 0) {
        int batch = buffer.size() - position;
        batch = batch < todo ? batch : static_cast<int>(todo);

        /*
         * bypass buffer.
         */
        if (0 == position && todo >= static_cast<int64_t>(buffer.size())) {
            checksum->update(buf + size - todo, batch);
            appendChunkToPacket(buf + size - todo, batch);
            bytesWritten += batch;
            checksum->reset();
        } else {
            checksum->update(buf + size - todo, batch);
            memcpy(&buffer[position], buf + size - todo, batch);
            position += batch;

            if (position == static_cast<int>(buffer.size())) {
                appendChunkToPacket(&buffer[0], buffer.size());
                bytesWritten += buffer.size();
                checksum->reset();
                position = 0;
            }
        }

        todo -= batch;

        if (currentPacket
                && (currentPacket->isFull() || bytesWritten == blockSize)) {
            sendPacket(currentPacket);

            if (isAppend) {
                isAppend = false;
                chunkSize = conf->getDefaultChunkSize();
                packetSize = conf->getDefaultPacketSize();
                computePacketChunkSize();
            }

            if (bytesWritten == blockSize) {
                closePipeline();
            }
        }
    }

    cursor += size;
}

void OutputStreamImpl::appendChunkToPacket(const char * buf, int size) {
    assert(NULL != buf && size > 0);

    if (!currentPacket) {
        currentPacket = packets.getPacket(packetSize, chunksPerPacket, bytesWritten,
                                          nextSeqNo++, checksumSize);
    }

    currentPacket->addChecksum(checksum->getValue());
    currentPacket->addData(buf, size);
    currentPacket->increaseNumChunks();
}

void OutputStreamImpl::sendPacket(shared_ptr<Packet> packet) {
    if (!pipeline) {
        setupPipeline();
    }

    pipeline->send(currentPacket);
    currentPacket.reset();
    lastSend = steady_clock::now();
}

void OutputStreamImpl::setupPipeline() {
    assert(currentPacket);
#ifdef MOCK
    pipeline = stub->getPipeline();
#else
    pipeline = shared_ptr<Pipeline>(new PipelineImpl(isAppend, path.c_str(), *conf, filesystem,
                                    CHECKSUM_TYPE_CRC32C, conf->getDefaultChunkSize(), replication,
                                    currentPacket->getOffsetInBlock(), packets, lastBlock));
#endif
    lastSend = steady_clock::now();
    /*
     * start heart beat beat thread
     */
    /*if (heartBeatStop) {
        if (heartBeatSender.joinable()) {
            heartBeatSender.join();
        }

        heartBeatStop = false;
        heartBeatSender = thread(&OutputStreamImpl::heartBeatSenderRoutine, this);
    }*/
}

/**
 * Flush all data in buffer and waiting for ack.
 * Will block until get all acks.
 */
void OutputStreamImpl::flush() {
    LOG(DEBUG3, "flush file %s at offset %" PRId64, path.c_str(), cursor);
    checkStatus();

    try {
        flushInternal(false);
    } catch (...) {
        setError(current_exception());
        throw;
    }
}

void OutputStreamImpl::flushInternal(bool needSync) {
    if (lastFlushed == cursor && !needSync) {
        return;
    } else {
        lastFlushed = cursor;
    }

    if (position > 0) {
        appendChunkToPacket(&buffer[0], position);
    }

    /*
     * if the pipeline and currentPacket are both NULL,
     * that means the pipeline has been closed and no more data in buffer/packet.
     * already synced when closing pipeline.
     */
    if (!currentPacket && needSync && pipeline) {
        currentPacket = packets.getPacket(packetSize, chunksPerPacket, bytesWritten,
                                          nextSeqNo++, checksumSize);
    }

    lock_guard < mutex > lock(mut);

    if (currentPacket) {
        currentPacket->setSyncFlag(needSync);
        sendPacket(currentPacket);
    }

    if (pipeline) {
        pipeline->flush();
    }
}

/**
 * return the current file length.
 * @return current file length.
 */
int64_t OutputStreamImpl::tell() {
    checkStatus();
    return cursor;
}

/**
 * @ref OutputStream::sync
 */
void OutputStreamImpl::sync() {
    LOG(DEBUG3, "sync file %s at offset %" PRId64, path.c_str(), cursor);
    checkStatus();

    try {
        flushInternal(true);
    } catch (...) {
        setError(current_exception());
        throw;
    }
}

void OutputStreamImpl::completeFile(bool throwError) {
    steady_clock::time_point start = steady_clock::now();

    while (true) {
        try {
            bool success;
            success = filesystem->complete(path, lastBlock.get());

            if (success) {
                return;
            }
        } catch (HdfsIOException & e) {
            if (throwError) {
                NESTED_THROW(HdfsIOException,
                             "OutputStreamImpl: failed to complete file %s.",
                             path.c_str());
            } else {
                return;
            }
        }

        if (closeTimeout > 0) {
            steady_clock::time_point end = steady_clock::now();

            if (ToMilliSeconds(start, end) >= closeTimeout) {
                if (throwError) {
                    THROW(HdfsIOException,
                          "OutputStreamImpl: timeout when complete file %s, timeout interval %d ms.",
                          path.c_str(), closeTimeout);
                } else {
                    return;
                }
            }
        }

        try {
            sleep_for(milliseconds(400));
        } catch (...) {
        }
    }
}

/**
 * close the stream.
 */
void OutputStreamImpl::closePipeline() {
    lock_guard < mutex > lock(mut);

    if (!pipeline) {
        return;
    }

    if (currentPacket) {
        sendPacket(currentPacket);
    }

    currentPacket = packets.getPacket(packetSize, chunksPerPacket, bytesWritten, nextSeqNo++,
                                      checksumSize);

    if (syncBlock) {
        currentPacket->setSyncFlag(syncBlock);
    }

    lastBlock = pipeline->close(currentPacket);
    assert(lastBlock);
    currentPacket.reset();
    pipeline.reset();
    filesystem->fsync(path);
    bytesWritten = 0;
}

void OutputStreamImpl::close() {
    exception_ptr e;

    if (closed) {
        return;
    }

    try {
        //pipeline may be broken
        if (!lastError) {
            if (lastFlushed != cursor && position > 0) {
                appendChunkToPacket(&buffer[0], position);
            }

            if (lastFlushed != cursor && currentPacket) {
                sendPacket(currentPacket);
            }

            closePipeline();
            /*heartBeatStop = true;
             condHeartBeatSender.notify_all();

             if (heartBeatSender.joinable()) {
             heartBeatSender.join();
             }*/
            completeFile(true);
        }
    } catch (...) {
        e = current_exception();
    }

    LeaseRenewer::GetLeaseRenewer().StopRenew(filesystem);
    LOG(DEBUG3, "close file %s for write with length %" PRId64, path.c_str(), cursor);
    reset();

    if (e) {
        rethrow_exception(e);
    }
}

void OutputStreamImpl::reset() {
    blockSize = 0;
    bytesWritten = 0;
    checksum->reset();
    chunkSize = 0;
    chunksPerPacket = 0;
    closed = true;
    closeTimeout = 0;
    conf.reset();
    currentPacket.reset();
    cursor = 0;
    filesystem.reset();
    heartBeatInterval = 0;
    isAppend = false;
    lastBlock.reset();
    lastError = exception_ptr();
    lastFlushed = 0;
    nextSeqNo = 0;
    packetSize = 0;
    path.clear();
    pipeline.reset();
    position = 0;
    replication = 0;
    syncBlock = false;
}

std::string OutputStreamImpl::toString() {
    if (path.empty()) {
        return std::string("OutputStream for path ") + path;
    } else {
        return std::string("OutputStream (not opened)");
    }
}

/*void OutputStreamImpl::heartBeatSenderRoutine() {
    assert(heartBeatStop == false);

    while (!heartBeatStop) {
        try {
            unique_lock < mutex > lock(mut);
            condHeartBeatSender.wait_for(lock, milliseconds(1000));

            try {
                try {
                    if (pipeline
                            && ToMilliSeconds(lastSend, steady_clock::now())
                            >= heartBeatInterval) {
                        pipeline->send(shared_ptr < Packet > (new Packet()));
                        lastSend = steady_clock::now();
                    }
                } catch (...) {
                    NESTED_THROW(Hdfs::HdfsIOException, "Failed to send heart beat, path: %s",
                                 path.c_str());
                }
            } catch (...) {
                lastError = current_exception();
                throw;
            }
        } catch (const std::bad_alloc & e) {

             * keep quiet if we run out of memory, since writing log need memory,
             * that may cause the process terminated.

            break;
        } catch (const Hdfs::HdfsException & e) {
            LOG(LOG_ERROR, "Heart beat thread exit since %s",
                GetExceptionDetail(e));
        } catch (const std::exception & e) {
            LOG(LOG_ERROR, "Heart beat thread exit since %s",
                e.what());
        }
    }

    heartBeatStop = true;
}*/

}
}
