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
#ifndef _HDFS_LIBHDFS3_CLIENT_OUTPUTSTREAMINTER_H_
#define _HDFS_LIBHDFS3_CLIENT_OUTPUTSTREAMINTER_H_

#include "ExceptionInternal.h"
#include "FileSystemInter.h"
#include "Memory.h"
#include "Permission.h"

namespace Hdfs {
namespace Internal {

/**
 * A output stream used to write data to hdfs.
 */
class OutputStreamInter {
public:
    virtual ~OutputStreamInter() {
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
    virtual void open(shared_ptr<FileSystemInter> fs, const char * path, int flag,
                      const Permission & permission, bool createParent, int replication,
                      int64_t blockSize) = 0;

    /**
     * To append data to file.
     * @param buf the data used to append.
     * @param size the data size.
     */
    virtual void append(const char * buf, int64_t size) = 0;

    /**
     * Flush all data in buffer and waiting for ack.
     * Will block until get all acks.
     */
    virtual void flush() = 0;

    /**
     * return the current file length.
     * @return current file length.
     */
    virtual int64_t tell() = 0;

    /**
     * @ref OutputStream::sync
     */
    virtual void sync() = 0;

    /**
     * close the stream.
     */
    virtual void close() = 0;

    virtual std::string toString() = 0;

    virtual void setError(const exception_ptr & error) = 0;
};

}
}

#endif /* _HDFS_LIBHDFS3_CLIENT_OUTPUTSTREAMINTER_H_ */
