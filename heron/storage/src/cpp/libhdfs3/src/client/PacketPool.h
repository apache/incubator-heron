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
#ifndef _HDFS_LIBHDFS3_CLIENT_PACKETPOOL_H_
#define _HDFS_LIBHDFS3_CLIENT_PACKETPOOL_H_
#include "Memory.h"

#include <deque>

namespace Hdfs {
namespace Internal {

class Packet;

/*
 * A simple packet pool implementation.
 *
 * Packet is created here if no packet is available.
 * And then add to Pipeline's packet queue to wait for the ack.
 * The Pipeline's packet queue size is not larger than the PacketPool's max size,
 * otherwise the write operation will be pending for the ack.
 * Once the ack is received, packet will reutrn back to the PacketPool to reuse.
 */
class PacketPool {
public:
    PacketPool(int size);
    shared_ptr<Packet> getPacket(int pktSize, int chunksPerPkt,
                                 int64_t offsetInBlock, int64_t seqno, int checksumSize);
    void relesePacket(shared_ptr<Packet> packet);

    void setMaxSize(int size) {
        this->maxSize = size;
    }

    int getMaxSize() const {
        return maxSize;
    }

private:
    int maxSize;
    std::deque<shared_ptr<Packet> > packets;
};

}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_PACKETPOOL_H_ */
