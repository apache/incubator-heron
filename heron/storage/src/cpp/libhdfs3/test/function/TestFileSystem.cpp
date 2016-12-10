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
#include "client/FileSystemInter.h"
#include "client/OutputStream.h"
#include "client/Permission.h"
#include "DateTime.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "gtest/gtest.h"
#include "TestUtil.h"
#include "Thread.h"
#include "XmlConfig.h"

#include <ctime>

#ifndef TEST_HDFS_PREFIX
#define TEST_HDFS_PREFIX "./"
#endif

#define BASE_DIR TEST_HDFS_PREFIX"/testFileSystem/"

using namespace Hdfs;
using namespace Hdfs::Internal;

class TestFileSystem: public ::testing::Test {
public:
    TestFileSystem() :
        conf("function-test.xml") {
        conf.set("output.default.packetsize", 1024);
        conf.set("rpc.client.ping.interval", 1000);
        fs = shared_ptr<FileSystem>(new FileSystem(conf));
        superfs = shared_ptr<FileSystem>(new FileSystem(conf));
        fs->connect();
        superfs->connect(conf.getString("dfs.default.uri"), HDFS_SUPERUSER, NULL);
        superfs->setWorkingDirectory(fs->getWorkingDirectory().c_str());

        try {
            superfs->deletePath(BASE_DIR, true);
        } catch (...) {
        }

        superfs->mkdirs(BASE_DIR, 0755);
        superfs->setOwner(BASE_DIR, USER, NULL);
    }

    ~TestFileSystem() {
        try {
            superfs->deletePath(BASE_DIR, true);
            fs->disconnect();
            superfs->disconnect();
        } catch (...) {
        }
    }

protected:
    Config conf;
    shared_ptr<FileSystem> fs;
    shared_ptr<FileSystem> superfs;
};

static inline bool CheckPermission(const FileSystem & fs, const char * path,
                                   const Permission & mode) {
    FileStatus status = fs.getFileStatus(path);
    return status.getPermission() == mode;
}

TEST_F(TestFileSystem, mkdir) {
    Permission p(ALL, READ_WRITE, READ_WRITE);
    ASSERT_THROW(fs->mkdir(NULL, p), InvalidParameter);
    ASSERT_THROW(fs->mkdir("", p), InvalidParameter);
    EXPECT_NO_THROW(fs->mkdir(BASE_DIR"testa", p));
    EXPECT_NO_THROW(fs->mkdir(BASE_DIR"testb", p));
}

TEST_F(TestFileSystem, mkdirs) {
    EXPECT_THROW(fs->mkdirs("", 0644), InvalidParameter);
    EXPECT_NO_THROW(fs->mkdirs(BASE_DIR"testmkdirs", 0644));
    EXPECT_NO_THROW(fs->mkdirs(BASE_DIR"m/testmkdirs", 0644));
}

TEST_F(TestFileSystem, rename) {
    ASSERT_THROW(fs->rename(NULL, "retest"), InvalidParameter);
    ASSERT_THROW(fs->rename("/test1/testa", NULL), InvalidParameter);
    ASSERT_THROW(fs->rename(NULL, NULL), InvalidParameter);
}

TEST_F(TestFileSystem, getDefaultReplication) {
    ASSERT_NO_THROW(fs->getDefaultReplication());
    ASSERT_EQ(fs->getDefaultReplication(), 3);
}

TEST_F(TestFileSystem, getFileStatus) {
    OutputStream ous;
    EXPECT_THROW(fs->getFileStatus(BASE_DIR"mm"), FileNotFoundException);
    ous.open(*fs, BASE_DIR"testGetFileStatus", Create, 0644, false, 0, 2048);
    char buffer[2048];
    FillBuffer(buffer, sizeof(buffer), 0);
    ous.append(buffer, sizeof(buffer));
    ous.close();
    EXPECT_NO_THROW(fs->getFileStatus(BASE_DIR"testGetFileStatus"));
}

TEST_F(TestFileSystem, listDirectory) {
    fs->disconnect();
    ASSERT_THROW(fs->listDirectory(BASE_DIR), HdfsIOException);
    fs->connect();
    ASSERT_THROW(fs->listDirectory(""), InvalidParameter);
    const int dirs = 10000;

    for (int i = 0; i < dirs; ++i) {
        std::stringstream ss;
        ss.imbue(std::locale::classic());
        ss << BASE_DIR << "testListDir/" << i;
        fs->mkdirs(ss.str().c_str(), 0777);
    }

    DirectoryIterator it;
    EXPECT_NO_THROW(it = fs->listDirectory(BASE_DIR"testListDir/"));
    int count = 0;

    while (it.hasNext()) {
        count ++;
        it.getNext();
    }

    ASSERT_EQ(dirs, count);
    ASSERT_THROW(it.getNext(), HdfsIOException);
}

TEST_F(TestFileSystem, setOwner) {
    fs->disconnect();
    ASSERT_THROW(fs->setOwner(BASE_DIR, "setOwner", ""), HdfsIOException);
    fs->connect();
    ASSERT_THROW(fs->setOwner(NULL, "", ""), InvalidParameter);
    EXPECT_THROW(fs->setOwner(BASE_DIR, "", ""), InvalidParameter);
    EXPECT_NO_THROW(superfs->setOwner(BASE_DIR, "setOwner", ""));
}

TEST_F(TestFileSystem, setTimes) {
    time_t now_time;
    now_time = time(NULL);
    fs->disconnect();
    ASSERT_THROW(fs->setTimes("", now_time, now_time), HdfsIOException);
    fs->connect();
    ASSERT_THROW(fs->setTimes(NULL, now_time, now_time), InvalidParameter);
    EXPECT_NO_THROW(fs->setTimes(BASE_DIR, now_time, now_time));
}

TEST_F(TestFileSystem, setPermission) {
    fs->disconnect();
    ASSERT_THROW(fs->setPermission("", 0644), HdfsIOException);
    fs->connect();
    ASSERT_THROW(fs->setPermission(NULL, 0644), InvalidParameter);
    EXPECT_THROW(superfs->setPermission("qq", 0644), FileNotFoundException);
}

TEST_F(TestFileSystem, setReplication) {
    fs->disconnect();
    ASSERT_THROW(fs->setReplication("", 4), HdfsIOException);
    fs->connect();
    ASSERT_THROW(fs->setReplication(NULL, 4), InvalidParameter);
    EXPECT_TRUE(false == fs->setReplication("qq", 0644));
}

TEST_F(TestFileSystem, setWorkingDirectory) {
    EXPECT_THROW(fs->setWorkingDirectory(""), InvalidParameter);
    EXPECT_THROW(fs->setWorkingDirectory("ddd"), InvalidParameter);
    EXPECT_NO_THROW(fs->setWorkingDirectory("/user/test1"));
}

TEST_F(TestFileSystem, getWorkingDirectory) {
    EXPECT_NO_THROW(fs->setWorkingDirectory("/user/test1"));
    EXPECT_STREQ("/user/test1", fs->getWorkingDirectory().c_str());
}

TEST_F(TestFileSystem, exist) {
    fs->disconnect();
    EXPECT_THROW(fs->exist(""), HdfsIOException);
    fs->connect();
    EXPECT_THROW(fs->exist(""), InvalidParameter);
    //EXPECT_NO_THROW(fs->exist(BASE_DIR"mm"));
    EXPECT_EQ(fs->exist("/mm"), false);
    EXPECT_EQ(fs->exist(BASE_DIR), true);
}

TEST_F(TestFileSystem, getStats) {
    fs->disconnect();
    EXPECT_THROW(fs->getStats(), HdfsIOException);
    fs->connect();
    EXPECT_NO_THROW(fs->getStats());
}

TEST_F(TestFileSystem, truncate) {
    OutputStream os;

    try {
        fs->truncate("NOTEXIST", 20);
    } catch (RpcNoSuchMethodException & e) {
        return;
    } catch (UnsupportedOperationException & e) {
        return;
    } catch (...) {
    }

    fs->disconnect();
    EXPECT_THROW(fs->truncate(BASE_DIR"description", 20), HdfsIOException);
    fs->connect();
    EXPECT_THROW(fs->truncate("", 20), InvalidParameter);
    EXPECT_THROW(fs->truncate(BASE_DIR"mm", 20), FileNotFoundException);
    EXPECT_NO_THROW(os.open(*fs, BASE_DIR"testTruncate", Create, 0644, false, 0, 2048));
    EXPECT_NO_THROW(os.close());
    EXPECT_THROW(fs->truncate(BASE_DIR"testTruncate", 20), InvalidParameter);
    EXPECT_NO_THROW(os.open(*fs, BASE_DIR"testTruncate", Append, 0644, false, 0, 2048));
    char buffer[512];
    FillBuffer(buffer, sizeof(buffer), 0);
    EXPECT_NO_THROW(os.append(buffer, sizeof(buffer)));
    EXPECT_NO_THROW(os.close());
    EXPECT_NO_THROW(fs->truncate(BASE_DIR"testTruncate", 20));
}

TEST_F(TestFileSystem, testPing) {
    EXPECT_NO_THROW(fs->listDirectory(BASE_DIR));
    sleep_for(seconds(5));
    EXPECT_NO_THROW(fs->listDirectory(BASE_DIR));
}

TEST_F(TestFileSystem, getFileBlockLocations) {
    EXPECT_THROW(fs->getFileBlockLocations(NULL, 0, 0), InvalidParameter);
    EXPECT_THROW(fs->getFileBlockLocations("", 0, 0), InvalidParameter);
    EXPECT_THROW(fs->getFileBlockLocations("NOTEXIST", -1, 0), InvalidParameter);
    EXPECT_THROW(fs->getFileBlockLocations("NOTEXIST", 0, -1), InvalidParameter);
    EXPECT_THROW(fs->getFileBlockLocations("NOTEXIST", 0, 0), FileNotFoundException);
    OutputStream os;
    std::vector<BlockLocation> retval;
    /**
     * empty file
     */
    os.open(*fs, BASE_DIR"TestGetFileBlockLocations", Create | Overwrite, 0777, true, 1, 1024);
    os.close();
    EXPECT_NO_THROW(DebugException(retval = fs->getFileBlockLocations(BASE_DIR"TestGetFileBlockLocations", 0, 100)));
    ASSERT_EQ(0u, retval.size());
    /**
     * opened file
     */
    os.open(*fs, BASE_DIR"TestGetFileBlockLocations", Create | Overwrite, 0777, true, 1, 1024);
    std::vector<char> buffer(1025);
    os.append(&buffer[0], buffer.size());
    os.sync();
    EXPECT_NO_THROW(DebugException(retval = fs->getFileBlockLocations(BASE_DIR"TestGetFileBlockLocations", 0, buffer.size())));
    ASSERT_EQ(2u, retval.size());

    for (size_t i = 0; i < retval.size(); ++i) {
        EXPECT_EQ(1u, retval[i].getHosts().size());
        EXPECT_EQ(1u, retval[i].getNames().size());
        EXPECT_EQ(1u, retval[i].getTopologyPaths().size());
        EXPECT_FALSE(retval[0].isCorrupt());
    }

    EXPECT_EQ(0, retval[0].getOffset());
    EXPECT_EQ(1024, retval[1].getOffset());
    EXPECT_EQ(1024, retval[0].getLength());
    //EXPECT_EQ(1, retval[1].getLength());
    os.close();
    /*
     * nonexistent regions
     */
    EXPECT_NO_THROW(DebugException(retval = fs->getFileBlockLocations(BASE_DIR"TestGetFileBlockLocations", buffer.size(), 100)));
    ASSERT_EQ(0u, retval.size());
}
