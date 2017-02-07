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
#include "client/FileSystem.h"
#include "client/InputStream.h"
#include "client/OutputStream.h"
#include "DateTime.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "gtest/gtest.h"
#include "Memory.h"
#include "TestUtil.h"
#include "Thread.h"
#include "XmlConfig.h"

#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef TEST_HDFS_PREFIX
#define TEST_HDFS_PREFIX "./"
#endif

#define BASE_DIR TEST_HDFS_PREFIX"/testOutputStream/"

using namespace Hdfs;
using namespace Hdfs::Internal;

class TestOutputStream: public ::testing::Test {
public:
    TestOutputStream() :
        conf("function-test.xml") {
        conf.set("output.default.packetsize", 1024);
        fs = new FileSystem(conf);
        fs->connect();
        superfs = new FileSystem(conf);
        superfs->connect(conf.getString("dfs.default.uri"), HDFS_SUPERUSER, NULL);
        superfs->setWorkingDirectory(fs->getWorkingDirectory().c_str());

        try {
            superfs->deletePath(BASE_DIR, true);
        } catch (...) {
        }

        superfs->mkdirs(BASE_DIR, 0755);
        superfs->setOwner(TEST_HDFS_PREFIX, USER, NULL);
        superfs->setOwner(BASE_DIR, USER, NULL);
    }

    ~TestOutputStream() {
        try {
            superfs->deletePath(BASE_DIR, true);
        } catch (...) {
        }

        fs->disconnect();
        delete fs;
        superfs->disconnect();
        delete superfs;
    }

    /**
     * size the size will be the size of a chunk or the size of a packet or the size of a block, the default blocksize is 2048
     */
    void CheckWrite(size_t size, int flag) {
        char buffer[size], buffer2[size - 100], buffer3[size + 100], readBuffer[2560];
        //check write a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer, sizeof(buffer), 0);
        ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
        ASSERT_NO_THROW(ous.close());
        ASSERT_NO_THROW(ins.open(*fs, BASE_DIR"testWrite", true));
        ASSERT_NO_THROW(ins.readFully(readBuffer, sizeof(buffer)));
        ASSERT_EQ(CheckBuffer(readBuffer, sizeof(buffer), 0), true);
        ASSERT_NO_THROW(ins.close());
        ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), true);
        //check write less than a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer2, sizeof(buffer2), 0);
        ASSERT_NO_THROW(ous.append(buffer2, sizeof(buffer2)));
        ASSERT_NO_THROW(ous.close());
        ASSERT_NO_THROW(ins.open(*fs, BASE_DIR"testWrite", false));
        ASSERT_NO_THROW(ins.readFully(readBuffer, sizeof(buffer2)));
        ASSERT_EQ(CheckBuffer(readBuffer, sizeof(buffer2), 0), true);
        ASSERT_NO_THROW(ins.close());
        ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), true);
        //check write greater than a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer3, sizeof(buffer3), 0);
        ASSERT_NO_THROW(ous.append(buffer3, sizeof(buffer3)));
        ASSERT_NO_THROW(ous.close());
        ASSERT_NO_THROW(ins.open(*fs, BASE_DIR"testWrite", true));
        ASSERT_NO_THROW(ins.readFully(readBuffer, sizeof(buffer3)));
        ASSERT_EQ(CheckBuffer(readBuffer, sizeof(buffer3), 0), true);
        ASSERT_NO_THROW(ins.close());
        ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), true);
    }

    void CheckOverWrite(size_t size, int flag) {
        char buffer[size], buffer2[size - 100], buffer3[size + 100], readBuffer[2560];

        //check overwrite a chunk|packet|block
        if (flag == Overwrite) {
            ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", Create, 0644, false, 0, 2048));
            ASSERT_NO_THROW(ous.close());
        }

        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer, sizeof(buffer), 0);
        ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
        ASSERT_NO_THROW(ous.close());
        ASSERT_NO_THROW(ins.open(*fs, BASE_DIR"testWrite", true));
        ASSERT_NO_THROW(ins.readFully(readBuffer, sizeof(buffer)));
        ASSERT_EQ(CheckBuffer(readBuffer, sizeof(buffer), 0), true);
        ASSERT_NO_THROW(ins.close());
        //check overwrite less than a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer2, sizeof(buffer2), 0);
        ASSERT_NO_THROW(ous.append(buffer2, sizeof(buffer2)));
        ASSERT_NO_THROW(ous.close());
        ASSERT_NO_THROW(ins.open(*fs, BASE_DIR"testWrite", false));
        ASSERT_NO_THROW(ins.readFully(readBuffer, sizeof(buffer2)));
        ASSERT_EQ(CheckBuffer(readBuffer, sizeof(buffer2), 0), true);
        ASSERT_NO_THROW(ins.close());
        //check overwrite greater than a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer3, sizeof(buffer3), 0);
        ASSERT_NO_THROW(ous.append(buffer3, sizeof(buffer3)));
        ASSERT_NO_THROW(ous.close());
        ASSERT_NO_THROW(ins.open(*fs, BASE_DIR"testWrite", true));
        ASSERT_NO_THROW(ins.readFully(readBuffer, sizeof(buffer3)));
        ASSERT_EQ(CheckBuffer(readBuffer, sizeof(buffer3), 0), true);
        ASSERT_NO_THROW(ins.close());
        ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), true);
    }

    void TestWrite(size_t size, int flag) {
        char buffer[size], buffer2[size - 100], buffer3[size + 100];
        FillBuffer(buffer, sizeof(buffer), 0);
        //when outputstream is not opened, test the append function
        ASSERT_THROW(ous.append(buffer, sizeof(buffer)), HdfsIOException);
        //Test write a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer, sizeof(buffer), 0);
        ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
        //Test write less than a chunk|packet|block
        FillBuffer(buffer2, sizeof(buffer2), 0);
        ASSERT_NO_THROW(ous.append(buffer2, sizeof(buffer2)));
        //Test write greater than a chunk|packet|block
        FillBuffer(buffer3, sizeof(buffer3), 0);
        ASSERT_NO_THROW(ous.append(buffer3, sizeof(buffer3)));
        ASSERT_NO_THROW(ous.close());
        ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), true);
        //Test write a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer, sizeof(buffer), 0);
        ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
        ASSERT_NO_THROW(ous.close());

        //Test write less than a chunk|packet|block
        if (flag == Create) {
            ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), true);
        }

        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer2, sizeof(buffer2), 0);
        ASSERT_NO_THROW(ous.append(buffer2, sizeof(buffer2)));
        ASSERT_NO_THROW(ous.close());

        //Test write greater than a chunk|packet|block
        if (flag == Create) {
            ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), true);
        }

        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer3, sizeof(buffer3), 0);
        ASSERT_NO_THROW(ous.append(buffer3, sizeof(buffer3)));
        ASSERT_NO_THROW(ous.close());
    }

    void TestAppend(size_t size, int flag) {
        char buffer[size], buffer2[size - 100], buffer3[size + 100], readBuffer[2560];
        //Test Append a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer, sizeof(buffer), 0);
        ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
        //Test write less than a chunk|packet|block
        FillBuffer(buffer2, sizeof(buffer2), 1);
        ASSERT_NO_THROW(ous.append(buffer2, sizeof(buffer2)));
        //Test write greater than a chunk|packet|block
        FillBuffer(buffer3, sizeof(buffer3), 2);
        ASSERT_NO_THROW(ous.append(buffer3, sizeof(buffer3)));
        ASSERT_NO_THROW(ous.close());
        ins.open(*fs, BASE_DIR"testWrite", true);
        ins.readFully(readBuffer, sizeof(buffer));
        ASSERT_EQ(CheckBuffer(readBuffer, sizeof(buffer), 0), true);
        ins.seek(sizeof(buffer));
        ins.readFully(readBuffer, sizeof(buffer2));
        ASSERT_EQ(CheckBuffer(readBuffer, sizeof(buffer2), 1), true);
        ins.seek(sizeof(buffer2) + sizeof(buffer));
        ins.readFully(readBuffer, sizeof(buffer3));
        ASSERT_EQ(CheckBuffer(readBuffer, sizeof(buffer3), 2), true);
        //Test write a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer, sizeof(buffer), 0);
        ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
        ASSERT_NO_THROW(ous.close());
        //Test write less than a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer2, sizeof(buffer2), 1);
        ASSERT_NO_THROW(ous.append(buffer2, sizeof(buffer2)));
        ASSERT_NO_THROW(ous.close());
        //Test write greater than a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer3, sizeof(buffer3), 2);
        ASSERT_NO_THROW(ous.append(buffer3, sizeof(buffer3)));
        ASSERT_NO_THROW(ous.close());
        ins.seek(sizeof(buffer) + sizeof(buffer2) + sizeof(buffer3));
        ins.readFully(readBuffer, sizeof(buffer));
        ASSERT_EQ(CheckBuffer(readBuffer, sizeof(buffer), 0), true);
        ins.seek(2 * sizeof(buffer) + sizeof(buffer2) + sizeof(buffer3));
        ins.readFully(readBuffer, sizeof(buffer2));
        ASSERT_EQ(CheckBuffer(readBuffer, sizeof(buffer2), 1), true);
        ins.seek(2 * sizeof(buffer) + 2 * sizeof(buffer2) + sizeof(buffer3));
        ins.readFully(readBuffer, sizeof(buffer3));
        ASSERT_EQ(CheckBuffer(readBuffer, sizeof(buffer3), 2), true);
        ins.close();
    }

    void TestAppendSyncBlock(size_t size, int flag) {
        char buffer[size], buffer2[size - 100], buffer3[size + 100];
        //Test Append a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer, sizeof(buffer), 0);
        ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
        ASSERT_NO_THROW(ous.sync());
        //Test write less than a chunk|packet|block
        FillBuffer(buffer2, sizeof(buffer2), 0);
        ASSERT_NO_THROW(ous.append(buffer2, sizeof(buffer2)));
        ASSERT_NO_THROW(ous.sync());
        //Test write greater than a chunk|packet|block
        FillBuffer(buffer3, sizeof(buffer3), 0);
        ASSERT_NO_THROW(ous.append(buffer3, sizeof(buffer3)));
        ASSERT_NO_THROW(ous.sync());
        ASSERT_NO_THROW(ous.close());
        //Test write a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer, sizeof(buffer), 0);
        ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
        ASSERT_NO_THROW(ous.sync());
        ASSERT_NO_THROW(ous.close());
        //Test write less than a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer2, sizeof(buffer2), 0);
        ASSERT_NO_THROW(ous.append(buffer2, sizeof(buffer2)));
        ASSERT_NO_THROW(ous.sync());
        ASSERT_NO_THROW(ous.close());
        //Test write greater than a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer3, sizeof(buffer3), 0);
        ASSERT_NO_THROW(ous.append(buffer3, sizeof(buffer3)));
        ASSERT_NO_THROW(ous.sync());
        ASSERT_NO_THROW(ous.close());
    }

    void TestCreateSyncBlock(size_t size, int flag) {
        char buffer[size], buffer2[size - 100], buffer3[size + 100];
        //Test Append a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer, sizeof(buffer), 0);
        ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
        ASSERT_NO_THROW(ous.sync());
        //Test write less than a chunk|packet|block
        FillBuffer(buffer2, sizeof(buffer2), 0);
        ASSERT_NO_THROW(ous.append(buffer2, sizeof(buffer2)));
        ASSERT_NO_THROW(ous.sync());
        //Test write greater than a chunk|packet|block
        FillBuffer(buffer3, sizeof(buffer3), 0);
        ASSERT_NO_THROW(ous.append(buffer3, sizeof(buffer3)));
        ASSERT_NO_THROW(ous.sync());
        ASSERT_NO_THROW(ous.close());
        //Test write a chunk|packet|block
        ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), true);
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer, sizeof(buffer), 0);
        ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
        ASSERT_NO_THROW(ous.sync());
        ASSERT_NO_THROW(ous.close());
        //Test write less than a chunk|packet|block
        ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), true);
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer2, sizeof(buffer2), 0);
        ASSERT_NO_THROW(ous.append(buffer2, sizeof(buffer2)));
        ASSERT_NO_THROW(ous.sync());
        ASSERT_NO_THROW(ous.close());
        //Test write greater than a chunk|packet|block
        ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), true);
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer3, sizeof(buffer3), 0);
        ASSERT_NO_THROW(ous.append(buffer3, sizeof(buffer3)));
        ASSERT_NO_THROW(ous.sync());
        ASSERT_NO_THROW(ous.close());
        ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), true);
    }

    void TestOverwriteSyncBlock(size_t size, int flag) {
        char buffer[size], buffer2[size - 100], buffer3[size + 100];
        //Test Append a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer, sizeof(buffer), 0);
        ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
        ASSERT_NO_THROW(ous.sync());
        //Test write less than a chunk|packet|block
        FillBuffer(buffer2, sizeof(buffer2), 0);
        ASSERT_NO_THROW(ous.append(buffer2, sizeof(buffer2)));
        ASSERT_NO_THROW(ous.sync());
        //Test write greater than a chunk|packet|block
        FillBuffer(buffer3, sizeof(buffer3), 0);
        ASSERT_NO_THROW(ous.append(buffer3, sizeof(buffer3)));
        ASSERT_NO_THROW(ous.sync());
        ASSERT_NO_THROW(ous.close());
        //Test write a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer, sizeof(buffer), 0);
        ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
        ASSERT_NO_THROW(ous.sync());
        ASSERT_NO_THROW(ous.close());
        //Test write less than a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer2, sizeof(buffer2), 0);
        ASSERT_NO_THROW(ous.append(buffer2, sizeof(buffer2)));
        ASSERT_NO_THROW(ous.sync());
        ASSERT_NO_THROW(ous.close());
        //Test write greater than a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer3, sizeof(buffer3), 0);
        ASSERT_NO_THROW(ous.append(buffer3, sizeof(buffer3)));
        ASSERT_NO_THROW(ous.sync());
        ASSERT_NO_THROW(ous.close());
    }

    void TestFlush(size_t size, int flag) {
        char buffer[size], buffer2[size - 100], buffer3[size + 100];
        //Test Append a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer, sizeof(buffer), 0);
        ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
        ASSERT_NO_THROW(ous.flush());
        //Test write less than a chunk|packet|block
        FillBuffer(buffer2, sizeof(buffer2), 0);
        ASSERT_NO_THROW(ous.append(buffer2, sizeof(buffer2)));
        ASSERT_NO_THROW(ous.flush());
        //Test write greater than a chunk|packet|block
        FillBuffer(buffer3, sizeof(buffer3), 0);
        ASSERT_NO_THROW(ous.append(buffer3, sizeof(buffer3)));
        ASSERT_NO_THROW(ous.flush());
        ASSERT_NO_THROW(ous.close());
        //Test write a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer, sizeof(buffer), 0);
        ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
        ASSERT_NO_THROW(ous.flush());
        ASSERT_NO_THROW(ous.close());
        //Test write less than a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer2, sizeof(buffer2), 0);
        ASSERT_NO_THROW(ous.append(buffer2, sizeof(buffer2)));
        ASSERT_NO_THROW(ous.flush());
        ASSERT_NO_THROW(ous.close());
        //Test write greater than a chunk|packet|block
        ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", flag, 0644, false, 0, 2048));
        FillBuffer(buffer3, sizeof(buffer3), 0);
        ASSERT_NO_THROW(ous.append(buffer3, sizeof(buffer3)));
        ASSERT_NO_THROW(ous.flush());
        ASSERT_NO_THROW(ous.close());
    }

protected:
    Config conf;
    FileSystem * fs;
    FileSystem * superfs;
    InputStream ins;
    OutputStream ous;
};

TEST_F(TestOutputStream, TestOpenFile_OpenFailed) {
    {
        //invalid path
        OutputStream os;
        EXPECT_THROW(os.open(*fs, "", Create), InvalidParameter);
    }
    {
        //invalid path
        OutputStream os;
        EXPECT_THROW(os.open(*fs, NULL, Create), InvalidParameter);
    }
    {
        //unconnect filesystem
        FileSystem fs(conf);
        OutputStream os;
        EXPECT_THROW(os.open(fs, BASE_DIR"a", Create), HdfsIOException);
    }
    {
        //path already exist as directory.
        OutputStream os;
        EXPECT_THROW(os.open(*fs, BASE_DIR, Create), FileAlreadyExistsException);
    }
    {
        //invalid flag
        OutputStream os;
        EXPECT_THROW(os.open(*fs, BASE_DIR"a", Append | Overwrite),
                     InvalidParameter);
    }
    {
        //invalid flag
        OutputStream os;
        EXPECT_THROW(os.open(*fs, BASE_DIR"a", Create | Append | Overwrite),
                     InvalidParameter);
    }
    {
        //invalid flag
        OutputStream os;
        EXPECT_THROW(os.open(*fs, BASE_DIR"a", 0100000000), InvalidParameter);
    }
    {
        //invalid flag
        OutputStream os;
        EXPECT_THROW(os.open(*fs, BASE_DIR"a", -1), InvalidParameter);
    }
    {
        //invalid flag
        OutputStream os;
        EXPECT_THROW(os.open(*fs, BASE_DIR"a", 0), InvalidParameter);
    }
    {
        //invalid permission.
        OutputStream os;
        EXPECT_THROW(os.open(*fs, BASE_DIR"a", Create, (1u << 10)),
                     InvalidParameter);
    }
    {
        //invalid replica number.
        OutputStream os;
        EXPECT_THROW(os.open(*fs, BASE_DIR"a", Create, 0644, false, -1),
                     InvalidParameter);
    }
    {
        //invalid block size.
        OutputStream os;
        EXPECT_THROW(os.open(*fs, BASE_DIR"a", Create, 0644, false, 0, -1),
                     InvalidParameter);
    }
    {
        //invalid block size.
        OutputStream os;
        EXPECT_THROW(os.open(*fs, BASE_DIR"a", Create, 0644, false, 0, 1234),
                     InvalidParameter);
    }
    {
        //Overwrite non-exist file
        OutputStream os;
        EXPECT_THROW(os.open(*fs, BASE_DIR"a", Overwrite),
                     FileNotFoundException);
    }
}

TEST_F(TestOutputStream, TestOpenFileForWrite) {
    ASSERT_NO_THROW(
        ous.open(*fs, BASE_DIR"//////b/c/d/././e/../../../../a", Create));
    //open an opened file
    OutputStream other;
    EXPECT_THROW(other.open(*fs, BASE_DIR"a", Create),
                 AlreadyBeingCreatedException);
    ous.close();
    //create an exist file
    ASSERT_THROW(ous.open(*fs, BASE_DIR"a", Create), FileAlreadyExistsException);
    //open for append
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"a", Append));
    ous.close();
    //overwrite an exist file
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"a", Overwrite));
    ous.close();
    //create or append an exist file
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"a", Create | Append));
    ous.close();
    //create or append a non-exist file
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"b", Create | Append));
    ous.close();
    //create new file with SyncBlock flag
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"c", Create | SyncBlock));
    ous.close();
    //append file with SyncBlock flag
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"c", Append | SyncBlock));
    ous.close();
    //overwrite file with SyncBlock flag
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"c", Overwrite | SyncBlock));
    ous.close();
}



TEST_F(TestOutputStream, TestWriteChunkPacket) {
    //test create a file and write a block
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", Create, 0644, false, 0, 2048));
    char buffer[512], buffer2[1024];
    FillBuffer(buffer, sizeof(buffer), 0);
    ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
    ASSERT_NO_THROW(ous.close());
    //test append a packet to a file
    ASSERT_THROW(ous.open(*fs, BASE_DIR"testWriteNotExist", Append, 0644, false, 0, 2048), FileNotFoundException);
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", Append, 0644, false, 0, 2048));
    FillBuffer(buffer2, sizeof(buffer2), 0);
    ASSERT_NO_THROW(ous.append(buffer2, sizeof(buffer2)));
    ASSERT_NO_THROW(ous.append(buffer2, sizeof(buffer2)));
    ASSERT_NO_THROW(ous.sync());
    ASSERT_NO_THROW(ous.close());
    //test overwrite a file
    ASSERT_THROW(ous.open(*fs, BASE_DIR"testWriteNotExist", Overwrite, 0644, false, 0, 2048), FileNotFoundException);
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", Overwrite, 0644, false, 0, 2048));
    FillBuffer(buffer, sizeof(buffer), 0);
    ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
    ASSERT_NO_THROW(ous.close());
    //test  create|Append
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", Create | Append, 0644, false, 0, 2048));
    FillBuffer(buffer, sizeof(buffer), 0);
    ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
    ASSERT_NO_THROW(ous.close());
    ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), 1);
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", Create | Append, 0644, false, 0, 2048));
    FillBuffer(buffer, sizeof(buffer), 0);
    ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
    ASSERT_NO_THROW(ous.close());
    //test create|Overwrite
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", Create | Overwrite, 0644, false, 0, 2048));
    FillBuffer(buffer, sizeof(buffer), 0);
    ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
    ASSERT_NO_THROW(ous.close());
    ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), 1);
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", Create | Overwrite, 0644, false, 0, 2048));
    FillBuffer(buffer, sizeof(buffer), 0);
    ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
    ASSERT_NO_THROW(ous.close());
    //test create|SyncBlock
    ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), 1);
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", Create | SyncBlock, 0644, false, 0, 2048));
    FillBuffer(buffer, sizeof(buffer), 0);
    ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
    ASSERT_NO_THROW(ous.sync());
    ASSERT_NO_THROW(ous.close());
    //test Append|SyncBlock
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", Append | SyncBlock, 0644, false, 0, 2048));
    FillBuffer(buffer, sizeof(buffer), 0);
    ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
    ASSERT_NO_THROW(ous.sync());
    ASSERT_NO_THROW(ous.close());
    //test Overwrite|SyncBlock
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", Overwrite | SyncBlock, 0644, false, 0, 2048));
    FillBuffer(buffer, sizeof(buffer), 0);
    ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
    ASSERT_NO_THROW(ous.sync());
    ASSERT_NO_THROW(ous.close());
}

TEST_F(TestOutputStream, CheckWriteChunk) {
    CheckWrite(512, Create | Append);
    CheckWrite(512, Create);
}

TEST_F(TestOutputStream, CheckWritePacket) {
    CheckWrite(1024, Create | Append);
    CheckWrite(1024, Create);
}

TEST_F(TestOutputStream, CheckWriteBlock) {
    CheckWrite(2048, Create | Append);
    CheckWrite(2048, Create);
}

TEST_F(TestOutputStream, CheckOverwrite) {
    CheckOverWrite(512, Overwrite);
    CheckOverWrite(1024, Overwrite);
    CheckOverWrite(2048, Overwrite);
    CheckOverWrite(512,  Create | Overwrite);
    CheckOverWrite(1024, Create | Overwrite);
    CheckOverWrite(2048, Create | Overwrite);
}



TEST_F(TestOutputStream, TestWrite) {
    TestWrite(512, Create);
    ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), true);
    TestWrite(1024, Create);
    ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), true);
    TestWrite(2048, Create);
    ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), true);
    TestWrite(512, Create | Append);
    ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), true);
    TestWrite(1024, Create | Append);
    ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), true);
    TestWrite(2048, Create | Append);
    ASSERT_EQ(fs->deletePath(BASE_DIR"testWrite", false), true);
}

TEST_F(TestOutputStream, TestAppend) {
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", Create, 0644, false, 0, 2048));
    ous.close();
    TestAppend(512, Append);
    fs->deletePath(BASE_DIR"testWrite", false);
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", Create, 0644, false, 0, 2048));
    ous.close();
    TestAppend(1024, Append);
    fs->deletePath(BASE_DIR"testWrite", false);
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", Create, 0644, false, 0, 2048));
    ous.close();
    TestAppend(2048, Append);
    fs->deletePath(BASE_DIR"testWrite", false);
}

TEST_F(TestOutputStream, TestAppendSyncBlock) {
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", Create, 0644, false, 0, 2048));
    ous.close();
    TestAppendSyncBlock(512, Append | SyncBlock);
    TestAppendSyncBlock(1024, Append | SyncBlock);
    TestAppendSyncBlock(2048, Append | SyncBlock);
    TestAppendSyncBlock(512, Overwrite | SyncBlock);
    TestAppendSyncBlock(1024, Overwrite | SyncBlock);
    TestAppendSyncBlock(2048, Overwrite | SyncBlock);
}

TEST_F(TestOutputStream, TestCreateSyncBlock) {
    TestCreateSyncBlock(512, Create | SyncBlock);
    TestCreateSyncBlock(1024, Create | SyncBlock);
    TestCreateSyncBlock(2048, Create | SyncBlock);
}

TEST_F(TestOutputStream, TestOverwriteSyncBlock) {
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", Create, 0644, false, 0, 2048));
    ous.close();
    TestOverwriteSyncBlock(512, Overwrite | SyncBlock);
    TestOverwriteSyncBlock(1024, Overwrite | SyncBlock);
    TestOverwriteSyncBlock(2048, Overwrite | SyncBlock);
}

TEST_F(TestOutputStream, TestFlush) {
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", Create, 0644, false, 0, 2048));
    ous.close();
    TestFlush(512, Append);
    TestFlush(1024, Append);
    TestFlush(2048, Append);
}

TEST_F(TestOutputStream, TestTell) {
    char buffer[512];
    ASSERT_NO_THROW(ous.open(*fs, BASE_DIR"testWrite", Create, 0644, false, 0, 2048));
    FillBuffer(buffer, sizeof(buffer), 0);
    ASSERT_NO_THROW(ous.append(buffer, sizeof(buffer)));
    ASSERT_EQ(ous.tell(), sizeof(buffer));
    ous.close();
}

static void CheckFileContent(FileSystem * fs, std::string path, int64_t len, size_t offset) {
    InputStream in;
    EXPECT_NO_THROW(in.open(*fs, path.c_str(), true));
    std::vector<char> buff(20 * 1024);
    int rc, todo = len, batch;

    while (todo > 0) {
        batch = todo < static_cast<int>(buff.size()) ? todo : buff.size();
        batch = in.read(&buff[0], batch);
        EXPECT_TRUE(batch > 0);
        todo = todo - batch;
        rc = Hdfs::CheckBuffer(&buff[0], batch, offset);
        offset += batch;
        EXPECT_TRUE(rc);
    }

    EXPECT_NO_THROW(in.close());
}

static void WriteSameTime(FileSystem * fs, std::string path, int flag, int64_t writeSize) {
    std::vector<char> buffer(64 * 1024);
    int64_t todo, batch;
    size_t  offset = 0;
    todo = writeSize;
    OutputStream ousA;
    EXPECT_NO_THROW(DebugException(ousA.open(*fs, path.c_str(), flag, 0644, false, 0, 1024 * 1024)));

    while (todo > 0) {
        batch = todo < static_cast<int>(buffer.size()) ? todo : buffer.size();
        Hdfs::FillBuffer(&buffer[0], batch, offset);
        EXPECT_NO_THROW(DebugException(ousA.append(&buffer[0], batch)));
        todo -= batch;
        offset += batch;
    }

    ASSERT_NO_THROW(DebugException(ousA.close()));
    CheckFileContent(fs, path, writeSize, 0);
}

static void NothrowTestWriteSameTime(FileSystem * fs, std::string path,
                                     int flag, int64_t writeSize) {
    EXPECT_NO_THROW(WriteSameTime(fs, path, flag, writeSize));
}

TEST_F(TestOutputStream, TestWriteSameTime) {
    int flag = Create | Overwrite;
    int64_t writeSize = 20 * 1024 * 1024 + 234;
    std::vector<shared_ptr<thread> > threads;
    const char * filename = BASE_DIR"testWriteSameTime";

    for (int i = 0; i <= 50; ++i) {
        std::stringstream buffer;
        buffer.imbue(std::locale::classic());
        buffer << filename << i;
        threads.push_back(
            shared_ptr<thread>(
                new thread(NothrowTestWriteSameTime, fs, buffer.str(), flag, writeSize)));
    }

    for (size_t i = 0; i < threads.size(); ++i) {
        threads[i]->join();
    }
}

























