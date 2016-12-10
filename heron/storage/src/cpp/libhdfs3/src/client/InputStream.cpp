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
#include "FileSystemImpl.h"
#include "FileSystemInter.h"
#include "InputStream.h"
#include "InputStreamImpl.h"
#include "InputStreamInter.h"

using namespace Hdfs::Internal;

namespace Hdfs {

InputStream::InputStream() {
    impl = new Internal::InputStreamImpl;
}

InputStream::~InputStream() {
    delete impl;
}

/**
 * Open a file to read
 * @param fs hdfs file system.
 * @param path the file to be read.
 * @param verifyChecksum verify the checksum.
 */
void InputStream::open(FileSystem & fs, const char * path,
                       bool verifyChecksum) {
    if (!fs.impl) {
        THROW(HdfsIOException, "FileSystem: not connected.");
    }

    impl->open(fs.impl->filesystem, path, verifyChecksum);
}

/**
 * To read data from hdfs.
 * @param buf the buffer used to filled.
 * @param size buffer size.
 * @return return the number of bytes filled in the buffer, it may less than size.
 */
int32_t InputStream::read(char * buf, int32_t size) {
    return impl->read(buf, size);
}

/**
 * To read data from hdfs, block until get the given size of bytes.
 * @param buf the buffer used to filled.
 * @param size the number of bytes to be read.
 */
void InputStream::readFully(char * buf, int64_t size) {
    impl->readFully(buf, size);
}

int64_t InputStream::available() {
    return impl->available();
}

/**
 * To move the file point to the given position.
 * @param pos the given position.
 */
void InputStream::seek(int64_t pos) {
    impl->seek(pos);
}

/**
 * To get the current file point position.
 * @return the position of current file point.
 */
int64_t InputStream::tell() {
    return impl->tell();
}

/**
 * Close the sthream.
 */
void InputStream::close() {
    impl->close();
}

}
