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
#include "gtest/gtest.h"
#include "client/hdfs.h"
#include "Logger.h"
#include "SessionConfig.h"
#include "TestUtil.h"
#include "XmlConfig.h"

#include <errno.h>
#include <fcntl.h>
#include <stdlib.h>
#include <limits>

using namespace Hdfs::Internal;

#ifndef TEST_HDFS_PREFIX
#define TEST_HDFS_PREFIX "./"
#endif

#define BASE_DIR TEST_HDFS_PREFIX"/testCInterface/"

using Hdfs::CheckBuffer;

static bool ReadFully(hdfsFS fs, hdfsFile file, char * buffer, size_t length) {
    int todo = length, rc;

    while (todo > 0) {
        rc = hdfsRead(fs, file, buffer + (length - todo), todo);

        if (rc <= 0) {
            return false;
        }

        todo = todo - rc;
    }

    return true;
}

static bool CreateFile(hdfsFS fs, const char * path, int64_t blockSize,
                       int64_t fileSize) {
    hdfsFile out;
    size_t offset = 0;
    int64_t todo = fileSize, batch;
    std::vector<char> buffer(32 * 1024);
    int rc = -1;

    do {
        if (NULL == (out = hdfsOpenFile(fs, path, O_WRONLY, 0, 0, blockSize))) {
            break;
        }

        while (todo > 0) {
            batch = todo < static_cast<int32_t>(buffer.size()) ?
                    todo : buffer.size();
            Hdfs::FillBuffer(&buffer[0], batch, offset);

            if (0 > (rc = hdfsWrite(fs, out, &buffer[0], batch))) {
                break;
            }

            todo -= rc;
            offset += rc;
        }

        rc = hdfsCloseFile(fs, out);
    } while (0);

    return rc >= 0;
}

bool CheckFileContent(hdfsFS fs, const char * path, int64_t len, size_t offset) {
    hdfsFile in = hdfsOpenFile(fs, path, O_RDONLY, 0, 0, 0);

    if (in == NULL) {
        return false;
    }

    std::vector<char> buff(1 * 1024 * 1024);
    int rc, todo = len, batch;

    while (todo > 0) {
        batch = todo < static_cast<int>(buff.size()) ? todo : buff.size();
        batch = hdfsRead(fs, in, &buff[0], batch);

        if (batch <= 0) {
            hdfsCloseFile(fs, in);
            return false;
        }

        todo = todo - batch;
        rc = Hdfs::CheckBuffer(&buff[0], batch, offset);
        offset += batch;

        if (!rc) {
            hdfsCloseFile(fs, in);
            return false;
        }
    }

    hdfsCloseFile(fs, in);
    return true;
}

int64_t GetFileLength(hdfsFS fs, const char * path) {
    int retval;
    hdfsFileInfo * info = hdfsGetPathInfo(fs, path);

    if (!info) {
        return -1;
    }

    retval = info->mSize;
    hdfsFreeFileInfo(info, 1);
    return retval;
}

TEST(TestCInterfaceConnect, TestConnect_InvalidInput) {
    hdfsFS fs = NULL;
    //test invalid input
    fs = hdfsConnect(NULL, 50070);
    EXPECT_TRUE(fs == NULL && EINVAL == errno);
    fs = hdfsConnect("hadoop.apache.org", 80);
    EXPECT_TRUE(fs == NULL && EIO == errno);
    fs = hdfsConnect("localhost", 22);
    EXPECT_TRUE(fs == NULL && EIO == errno);
}

static void ParseHdfsUri(const std::string & uri, std::string & host, int & port) {
    std::string str = uri;
    char * p = &str[0], *q;

    if (0 == strncasecmp(p, "hdfs://", strlen("hdfs://"))) {
        p += strlen("hdfs://");
    }

    q = strchr(p, ':');

    if (NULL == q) {
        port = 0;
    } else {
        *q++ = 0;
        port = strtol(q, NULL, 0);
    }

    host = p;
}

TEST(TestCInterfaceConnect, TestConnect_Success) {
    hdfsFS fs = NULL;
    char * uri = NULL;
    setenv("LIBHDFS3_CONF", "function-test.xml", 1);
    struct hdfsBuilder * bld = hdfsNewBuilder();
    assert(bld != NULL);
    hdfsBuilderSetNameNode(bld, "default");
    //test valid input
    fs = hdfsBuilderConnect(bld);
    ASSERT_TRUE(fs != NULL);
    ASSERT_EQ(hdfsDisconnect(fs), 0);
    hdfsBuilderSetUserName(bld, "test");
    fs = hdfsBuilderConnect(bld);
    ASSERT_TRUE(fs != NULL);
    ASSERT_EQ(hdfsDisconnect(fs), 0);
    hdfsFreeBuilder(bld);
    std::string host;
    int port;
    ASSERT_EQ(0, hdfsConfGetStr("dfs.default.uri", &uri));
    ParseHdfsUri(uri, host, port);
    hdfsConfStrFree(uri);
    fs = hdfsConnectAsUser(host.c_str(), port, USER);
    ASSERT_TRUE(fs != NULL);
    ASSERT_EQ(hdfsDisconnect(fs), 0);
    fs = hdfsConnectAsUserNewInstance(host.c_str(), port, USER);
    ASSERT_TRUE(fs != NULL);
    ASSERT_EQ(hdfsDisconnect(fs), 0);
    fs = hdfsConnectNewInstance(host.c_str(), port);
    ASSERT_TRUE(fs != NULL);
    ASSERT_EQ(hdfsDisconnect(fs), 0);
}


TEST(TestErrorMessage, TestErrorMessage) {
    EXPECT_NO_THROW(hdfsGetLastError());
    hdfsChown(NULL, TEST_HDFS_PREFIX, NULL, NULL);
    LOG(LOG_ERROR, "%s", GetSystemErrorInfo(EINVAL));
    EXPECT_STREQ(GetSystemErrorInfo(EINVAL), hdfsGetLastError());
}

class TestCInterface: public ::testing::Test {
public:
    TestCInterface() {
        setenv("LIBHDFS3_CONF", "function-test.xml", 1);
        struct hdfsBuilder * bld = hdfsNewBuilder();
        assert(bld != NULL);
        hdfsBuilderSetNameNode(bld, "default");
        hdfsBuilderSetForceNewInstance(bld);
        fs = hdfsBuilderConnect(bld);

        if (fs == NULL) {
            throw std::runtime_error("cannot connect hdfs");
        }

        hdfsBuilderSetUserName(bld, HDFS_SUPERUSER);
        superfs = hdfsBuilderConnect(bld);
        hdfsFreeBuilder(bld);

        if (superfs == NULL) {
            throw std::runtime_error("cannot connect hdfs");
        }

        std::vector<char> buffer(128);
        hdfsSetWorkingDirectory(superfs, hdfsGetWorkingDirectory(fs, &buffer[0], buffer.size()));
        hdfsDelete(superfs, BASE_DIR, true);

        if (0 != hdfsCreateDirectory(superfs, BASE_DIR)) {
            throw std::runtime_error("cannot create test directory");
        }

        if (0 != hdfsChown(superfs, TEST_HDFS_PREFIX, USER, NULL)) {
            throw std::runtime_error("cannot set owner for test directory");
        }

        if (0 != hdfsChown(superfs, BASE_DIR, USER, NULL)) {
            throw std::runtime_error("cannot set owner for test directory");
        }
    }

    ~TestCInterface() {
        hdfsDelete(superfs, BASE_DIR, true);
        hdfsDisconnect(fs);
        hdfsDisconnect(superfs);
    }

protected:
    hdfsFS fs;
    hdfsFS superfs;
};

TEST_F(TestCInterface, TestGetConf) {
    char * output = NULL;
    EXPECT_EQ(-1, hdfsConfGetStr(NULL, NULL));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(-1, hdfsConfGetStr("test.get.conf", NULL));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(-1, hdfsConfGetStr(NULL, &output));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(-1, hdfsConfGetStr("not exist", &output));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(0, hdfsConfGetStr("test.get.conf", &output));
    EXPECT_STREQ("success", output);
    hdfsConfStrFree(output);
}

TEST_F(TestCInterface, TestGetConfInt32) {
    int output;
    EXPECT_EQ(-1, hdfsConfGetInt(NULL, NULL));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(-1, hdfsConfGetInt("test.get.confint32", NULL));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(-1, hdfsConfGetInt(NULL, &output));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(-1, hdfsConfGetInt("not exist", &output));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(0, hdfsConfGetInt("test.get.confint32", &output));
    EXPECT_EQ(10, output);
}

TEST_F(TestCInterface, TestGetBlockSize) {
    EXPECT_EQ(-1, hdfsGetDefaultBlockSize(NULL));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_GT(hdfsGetDefaultBlockSize(fs), 0);
}

TEST_F(TestCInterface, TestGetCapacity) {
    EXPECT_EQ(-1, hdfsGetCapacity(NULL));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_GT(hdfsGetCapacity(fs), 0);
}

TEST_F(TestCInterface, TestGetUsed) {
    EXPECT_EQ(-1, hdfsGetUsed(NULL));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_GE(hdfsGetUsed(fs), 0);
}

TEST_F(TestCInterface, TestAvailable) {
    hdfsFile out = NULL, in = NULL;
    out = hdfsOpenFile(fs, BASE_DIR"/TestAvailable_InvalidInput", O_WRONLY | O_APPEND, 0, 0, 0);
    ASSERT_TRUE(NULL != out);
    in = hdfsOpenFile(fs, BASE_DIR"/TestAvailable_InvalidInput", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(NULL != in);
    EXPECT_EQ(-1, hdfsAvailable(NULL, NULL));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(-1, hdfsAvailable(fs, NULL));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(-1, hdfsAvailable(NULL, in));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_EQ(-1, hdfsAvailable(fs, out));
    EXPECT_EQ(EINVAL, errno);
    EXPECT_GE(hdfsAvailable(fs, in), 0);
    ASSERT_EQ(0, hdfsCloseFile(fs, in));
    ASSERT_EQ(0, hdfsCloseFile(fs, out));
}

TEST_F(TestCInterface, TestOpenFile_InvalidInput) {
    hdfsFile file = NULL;
    //test invalid input
    file = hdfsOpenFile(NULL, BASE_DIR"/testOpenFile", O_WRONLY, 0, 0, 0);
    EXPECT_TRUE(file == NULL && EINVAL == errno);
    file = hdfsOpenFile(fs, NULL, O_WRONLY, 0, 0, 0);
    EXPECT_TRUE(file == NULL && EINVAL == errno);
    file = hdfsOpenFile(fs, "", O_WRONLY, 0, 0, 0);
    EXPECT_TRUE(file == NULL && EINVAL == errno);
    //test O_RDWR flag
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_RDWR, 0, 0, 0);
    EXPECT_TRUE(file == NULL && ENOTSUP == errno);
    //test O_EXCL | O_CREATE flag
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_EXCL | O_CREAT, 0, 0, 0);
    EXPECT_TRUE(file == NULL && ENOTSUP == errno);
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_WRONLY, 0, -1, 0);
    EXPECT_TRUE(file == NULL && EINVAL == errno);
    //test invalid block size
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_WRONLY, 0, 0, 1);
    EXPECT_TRUE(file == NULL && EINVAL == errno);
    //open not exist file
    file = hdfsOpenFile(fs, BASE_DIR"/notExist", O_RDONLY, 0, 0, 0);
    EXPECT_TRUE(file == NULL && ENOENT == errno);
}

TEST_F(TestCInterface, TestOpenFile_Success) {
    hdfsFile file = NULL;
    hdfsFileInfo * info = NULL;
    //crate a file
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_CREAT, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    //append to a file
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_WRONLY | O_APPEND, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    //overwrite a file
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    //remove file
    ASSERT_EQ(hdfsDelete(fs, BASE_DIR"/testOpenFile", true), 0);
    //create a new file with block size, replica size
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_WRONLY, 0, 1, 1024);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    info = hdfsGetPathInfo(fs, BASE_DIR"/testOpenFile");
    ASSERT_TRUE(info != NULL);
    EXPECT_TRUE(1024 == info->mBlockSize && 0 == info->mSize && 1 == info->mReplication);
    hdfsFreeFileInfo(info, 1);
    //open for read
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    //test open a file for write, which has been opened for write, should success
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    hdfsFile another = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_WRONLY, 0, 1, 1024);
    ASSERT_TRUE(another != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    EXPECT_EQ(hdfsCloseFile(fs, another), 0);
    //test open a file for append, which has been opened for write, should fail.
    file = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    another = hdfsOpenFile(fs, BASE_DIR"/testOpenFile", O_WRONLY | O_APPEND, 0, 0, 0);
    EXPECT_TRUE(another == NULL && EBUSY == errno);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
}

TEST_F(TestCInterface, TestFileExist_InvalidInput) {
    int err;
    //test invalid input
    err = hdfsExists(NULL, BASE_DIR"/notExist");
    EXPECT_TRUE(err == -1 && EINVAL == errno);
    err = hdfsExists(fs, NULL);
    EXPECT_TRUE(err == -1 && EINVAL == errno);
    //test file which does not exist
    err = hdfsExists(fs, BASE_DIR"/notExist");
    EXPECT_EQ(err, -1);
}

TEST_F(TestCInterface, TestFileExist_Success) {
    hdfsFile file = NULL;
    //create a file and test
    file = hdfsOpenFile(fs, BASE_DIR"/testExists", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    EXPECT_EQ(hdfsExists(fs, BASE_DIR"/testExists"), 0);
}

TEST_F(TestCInterface, TestFileClose_InvalidInput) {
    int retval;
    hdfsFile file;
    file = hdfsOpenFile(fs, BASE_DIR"/testFileClose", O_WRONLY, 0, 0, 0);
    //test invalid input
    retval = hdfsCloseFile(NULL, file);
    EXPECT_TRUE(retval == -1 && EINVAL == errno);
    EXPECT_EQ(hdfsCloseFile(fs, NULL), 0);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
}

TEST_F(TestCInterface, TestFileClose_Success) {
    int retval;
    hdfsFile file;
    file = hdfsOpenFile(fs, BASE_DIR"/testFileClose", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    //test close file
    retval = hdfsCloseFile(fs, file);
    EXPECT_EQ(retval, 0);
    EXPECT_EQ(hdfsCloseFile(fs, NULL), 0);
}

TEST_F(TestCInterface, TestFileSeek_InvalidInput) {
    int err;
    hdfsFile file = NULL;
    //test invalid input
    err = hdfsSeek(NULL, file, 1);
    EXPECT_TRUE(-1 == err && EINVAL == errno);
    err = hdfsSeek(fs, NULL, 1);
    EXPECT_TRUE(-1 == err && EINVAL == errno);
    file = hdfsOpenFile(fs, BASE_DIR"/testFileSeek", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    EXPECT_TRUE(4 == hdfsWrite(fs, file, "abcd", 4));
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    file = hdfsOpenFile(fs, BASE_DIR"/testFileSeek", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(NULL != file);
    //seek over eof
    err = hdfsSeek(fs, file, 100);
    EXPECT_TRUE(-1 == err && EOVERFLOW == errno);
    //test seek a file which is opened for write
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    file = hdfsOpenFile(fs, BASE_DIR"/testFileSeek", O_WRONLY | O_APPEND, 0, 0,
                        0);
    ASSERT_TRUE(NULL != file);
    err = hdfsSeek(fs, file, 1);
    EXPECT_TRUE(-1 == err && EINVAL == errno);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
}

static void TestSeek(hdfsFS fs) {
    int err;
    hdfsFile file = NULL;
    int blockSize = 1024 * 1024;
    std::vector<char> buffer(8 * 1024);
    ASSERT_TRUE(CreateFile(fs, BASE_DIR"/testFileSeek", blockSize, blockSize * 21));
    file = hdfsOpenFile(fs, BASE_DIR"/testFileSeek", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    //seek to eof, we do expect to return success
    err = hdfsSeek(fs, file, blockSize * 21);
    EXPECT_EQ(0, err);
    //seek to 0
    err = hdfsSeek(fs, file, 0);
    EXPECT_EQ(0, err);
    EXPECT_EQ(0, hdfsTell(fs, file));
    EXPECT_TRUE(ReadFully(fs, file, &buffer[0], buffer.size()));
    EXPECT_TRUE(Hdfs::CheckBuffer(&buffer[0], buffer.size(), 0));
    //seek to current position
    err = hdfsSeek(fs, file, buffer.size());
    EXPECT_EQ(0, err);
    EXPECT_EQ(buffer.size(), hdfsTell(fs, file));
    EXPECT_TRUE(ReadFully(fs, file, &buffer[0], buffer.size()));
    EXPECT_TRUE(Hdfs::CheckBuffer(&buffer[0], buffer.size(), buffer.size()));
    //seek to next block
    err = hdfsSeek(fs, file, blockSize);
    EXPECT_EQ(0, err);
    EXPECT_EQ(blockSize, hdfsTell(fs, file));
    EXPECT_TRUE(ReadFully(fs, file, &buffer[0], buffer.size()));
    EXPECT_TRUE(Hdfs::CheckBuffer(&buffer[0], buffer.size(), blockSize));
    //seek to next 20 block
    err = hdfsSeek(fs, file, blockSize * 20);
    EXPECT_EQ(0, err);
    EXPECT_EQ(blockSize * 20, hdfsTell(fs, file));
    EXPECT_TRUE(ReadFully(fs, file, &buffer[0], buffer.size()));
    EXPECT_TRUE(Hdfs::CheckBuffer(&buffer[0], buffer.size(), blockSize * 20));
    //seek back to second block
    err = hdfsSeek(fs, file, blockSize);
    EXPECT_EQ(0, err);
    EXPECT_EQ(blockSize, hdfsTell(fs, file));
    EXPECT_TRUE(ReadFully(fs, file, &buffer[0], buffer.size()));
    EXPECT_TRUE(Hdfs::CheckBuffer(&buffer[0], buffer.size(), blockSize));
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
}

TEST_F(TestCInterface, TestFileSeek_Success) {
    TestSeek(fs);
    struct hdfsBuilder * bld = hdfsNewBuilder();
    assert(bld != NULL);
    hdfsBuilderSetNameNode(bld, "default");
    hdfsBuilderConfSetStr(bld, "dfs.client.read.shortcircuit", "false");
    hdfsBuilderSetForceNewInstance(bld);
    hdfsFS newfs = hdfsBuilderConnect(bld);
    hdfsFreeBuilder(bld);
    ASSERT_TRUE(newfs != NULL);
    TestSeek(newfs);
    hdfsDisconnect(newfs);
}

TEST_F(TestCInterface, TestFileTell_InvalidInput) {
    int64_t err;
    hdfsFile out = NULL;
    out = hdfsOpenFile(fs, BASE_DIR"/testFileTell", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(out != NULL);
    //test invalid input
    err = hdfsTell(NULL, out);
    EXPECT_TRUE(-1 == err && EINVAL == errno);
    err = hdfsTell(fs, NULL);
    EXPECT_TRUE(-1 == err && EINVAL == errno);
    hdfsCloseFile(fs, out);
}

TEST_F(TestCInterface, TestFileTell_Success) {
    hdfsFile in, out;
    int batch;
    size_t offset = 0;
    int64_t todo, fileSize = 21 * 1024 * 1024;
    std::vector<char> buffer(8 * 1024);
    out = hdfsOpenFile(fs, BASE_DIR"/testFileTell", O_WRONLY, 0, 0, 1024 * 1024);
    ASSERT_TRUE(out != NULL);
    ASSERT_EQ(0, hdfsTell(fs, out));
    srand(0);
    todo = fileSize;

    while (todo > 0) {
        batch = (rand() % (buffer.size() - 1)) + 1;
        batch = batch < todo ? batch : todo;
        Hdfs::FillBuffer(&buffer[0], batch, offset);
        ASSERT_EQ(batch, hdfsWrite(fs, out, &buffer[0], batch));
        todo -= batch;
        offset += batch;
        ASSERT_EQ(fileSize - todo, hdfsTell(fs, out));
    }

    EXPECT_EQ(0, hdfsCloseFile(fs, out));
    in = hdfsOpenFile(fs, BASE_DIR"/testFileTell", O_RDONLY, 0, 0, 0);
    EXPECT_TRUE(NULL != in);
    EXPECT_EQ(0, hdfsTell(fs, in));
    offset = 0;
    todo = fileSize;

    while (todo > 0) {
        batch = (rand() % (buffer.size() - 1)) + 1;
        batch = batch < todo ? batch : todo;
        batch = hdfsRead(fs, in, &buffer[0], batch);
        ASSERT_TRUE(batch > 0);
        todo -= batch;
        EXPECT_TRUE(Hdfs::CheckBuffer(&buffer[0], batch, offset));
        offset += batch;
        ASSERT_EQ(fileSize - todo, hdfsTell(fs, in));
    }

    EXPECT_EQ(0, hdfsCloseFile(fs, in));
}

TEST_F(TestCInterface, TestDelete_InvalidInput) {
    int err;
    //test invalid input
    err = hdfsDelete(NULL, BASE_DIR"/testFileDelete", 0);
    EXPECT_TRUE(-1 == err && EINVAL == errno);
    err = hdfsDelete(fs, NULL, 0);
    EXPECT_TRUE(-1 == err && EINVAL == errno);
    err = hdfsDelete(fs, "", 0);
    EXPECT_TRUE(-1 == err && EINVAL == errno);
}

TEST_F(TestCInterface, TestDelete_Success) {
    hdfsFile file = NULL;
    int err;
    file = hdfsOpenFile(fs, BASE_DIR"/testFileDelete", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    err = hdfsCreateDirectory(fs, BASE_DIR"/testFileDeleteDir");
    EXPECT_EQ(0, err);
    file = hdfsOpenFile(fs, BASE_DIR"/testFileDeleteDir/testFileDelete",
                        O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(NULL != file);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    //delete file
    err = hdfsDelete(fs, BASE_DIR"/testFileDelete", 0);
    EXPECT_EQ(0, err);
    err = hdfsExists(fs, BASE_DIR"/testFileDelete");
    EXPECT_NE(err, 0);
    //delete directory
    err = hdfsDelete(fs, BASE_DIR"/testFileDeleteDir", 1);
    EXPECT_EQ(0, err);
    err = hdfsExists(fs, BASE_DIR"/testFileDeleteDir/testFileDelete");
    EXPECT_NE(err, 0);
    err = hdfsExists(fs, BASE_DIR"/testFileDeleteDir");
    EXPECT_NE(err, 0);
}

TEST_F(TestCInterface, TestRename_InvalidInput) {
    hdfsFile file = NULL;
    int err;
    file = hdfsOpenFile(fs, BASE_DIR"/testFileRename", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(NULL != file);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    err = hdfsCreateDirectory(fs, BASE_DIR"/testFileRenameDir");
    EXPECT_EQ(0, err);
    //test invalid input
    err = hdfsRename(NULL, BASE_DIR"/testFileRename",
                     BASE_DIR"/testFileRenameNew");
    EXPECT_TRUE(0 != err && EINVAL == errno);
    err = hdfsRename(fs, NULL, BASE_DIR"/testFileRenameNew");
    EXPECT_TRUE(0 != err && EINVAL == errno);
    err = hdfsRename(fs, "", BASE_DIR"/testFileRenameNew");
    EXPECT_TRUE(0 != err && EINVAL == errno);
    err = hdfsRename(fs, BASE_DIR"/testFileRename", NULL);
    EXPECT_TRUE(0 != err && EINVAL == errno);
    err = hdfsRename(fs, BASE_DIR"/testFileRename", "");
    EXPECT_TRUE(0 != err && EINVAL == errno);
}

TEST_F(TestCInterface, TestRename_Success) {
    hdfsFile file = NULL;
    int err;
    file = hdfsOpenFile(fs, BASE_DIR"/testFileRename", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(NULL != file);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    err = hdfsCreateDirectory(fs, BASE_DIR"/testFileRenameDir");
    EXPECT_EQ(0, err);
    //rename a file
    err = hdfsRename(fs, BASE_DIR"/testFileRename",
                     BASE_DIR"/testFileRenameNew");
    EXPECT_EQ(0, err);
    err = hdfsExists(fs, BASE_DIR"/testFileRenameNew");
    EXPECT_EQ(0, err);
    //rename a directory
    err = hdfsRename(fs, BASE_DIR"/testFileRenameDir",
                     BASE_DIR"/testFileRenameDirNew");
    EXPECT_EQ(0, err);
    err = hdfsExists(fs, BASE_DIR"/testFileRenameDirNew");
    EXPECT_EQ(0, err);
}

TEST_F(TestCInterface, TestGetWorkingDirectory_InvalidInput) {
    char * ret, buffer[1024];
    //test invalid input
    ret = hdfsGetWorkingDirectory(NULL, buffer, sizeof(buffer));
    EXPECT_TRUE(ret == 0 && EINVAL == errno);
    ret = hdfsGetWorkingDirectory(fs, NULL, sizeof(buffer));
    EXPECT_TRUE(ret == 0 && EINVAL == errno);
    ret = hdfsGetWorkingDirectory(fs, buffer, 0);
    EXPECT_TRUE(ret == 0 && EINVAL == errno);
}

TEST_F(TestCInterface, TestGetWorkingDirectory_Success) {
    char * ret, buffer[1024];
    ret = hdfsGetWorkingDirectory(fs, buffer, sizeof(buffer));
    EXPECT_TRUE(ret != NULL);
    EXPECT_STREQ("/user/" USER, buffer);
}

TEST_F(TestCInterface, TestSetWorkingDirectory_InvalidInput) {
    int err;
    //test invalid input
    err = hdfsSetWorkingDirectory(NULL, BASE_DIR);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsSetWorkingDirectory(fs, NULL);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsSetWorkingDirectory(fs, "");
    EXPECT_TRUE(err != 0 && EINVAL == errno);
}

TEST_F(TestCInterface, TestSetWorkingDirectory_Success) {
    int err;
    char * ret, target[1024], buffer[1024];
    ret = hdfsGetWorkingDirectory(fs, target, sizeof(target));
    ASSERT_TRUE(ret != NULL);
    strcat(target, "/./" BASE_DIR);
    err = hdfsSetWorkingDirectory(fs, target);
    EXPECT_EQ(0, err);
    ret = hdfsGetWorkingDirectory(fs, buffer, sizeof(buffer));
    EXPECT_TRUE(ret != NULL && strcmp(buffer, target) == 0);
    err = hdfsCreateDirectory(fs, "testCreateDir");
    EXPECT_EQ(0, err);
    strcat(target, "testCreateDir");
    err = hdfsExists(fs, target);
    EXPECT_EQ(err, 0);
}

TEST_F(TestCInterface, TestCreateDir_InvalidInput) {
    int err = 0;
    //test invalid input
    err = hdfsCreateDirectory(NULL, BASE_DIR"/testCreateDir");
    EXPECT_TRUE(err == -1 && EINVAL == errno);
    err = hdfsCreateDirectory(fs, NULL);
    EXPECT_TRUE(err == -1 && EINVAL == errno);
    err = hdfsCreateDirectory(fs, "");
    EXPECT_TRUE(err == -1 && EINVAL == errno);
}

TEST_F(TestCInterface, TestCreateDir_Success) {
    int err = 0;
    err = hdfsCreateDirectory(fs, BASE_DIR"/testCreateDir");
    EXPECT_EQ(0, err);
    err = hdfsExists(fs, BASE_DIR"/testCreateDir");
    EXPECT_EQ(0, err);
    err = hdfsCreateDirectory(fs, BASE_DIR"/testCreate  Dir");
    EXPECT_EQ(0, err);
    err = hdfsExists(fs, BASE_DIR"/testCreate  Dir");
    EXPECT_EQ(0, err);
}

TEST_F(TestCInterface, TestSetReplication_InvalidInput) {
    hdfsFile file = NULL;
    hdfsFileInfo * info = NULL;
    int err;
    file = hdfsOpenFile(fs, BASE_DIR"/testSetReplication", O_WRONLY, 0, 1, 0);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    //test invalid input
    err = hdfsSetReplication(NULL, BASE_DIR"/testSetReplication", 2);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsSetReplication(fs, NULL, 2);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsSetReplication(fs, "", 2);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsSetReplication(fs, BASE_DIR"/testSetReplication", 0);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    hdfsFreeFileInfo(info, 1);
}

TEST_F(TestCInterface, TestSetReplication_Success) {
    hdfsFile file = NULL;
    hdfsFileInfo * info = NULL;
    int err;
    file = hdfsOpenFile(fs, BASE_DIR"/testSetReplication", O_WRONLY, 0, 1, 0);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    err = hdfsSetReplication(fs, BASE_DIR"/testSetReplication", 2);
    EXPECT_EQ(0, err);
    info = hdfsGetPathInfo(fs, BASE_DIR"/testSetReplication");
    EXPECT_TRUE(info != NULL);
    EXPECT_EQ(info->mReplication, 2);
    hdfsFreeFileInfo(info, 1);
}

TEST_F(TestCInterface, TestListDirectory_InvalidInput) {
    int num;
    hdfsFileInfo * info = NULL;
    //test invalid input
    info = hdfsListDirectory(NULL, BASE_DIR, &num);
    EXPECT_TRUE(info == NULL && EINVAL == errno);
    info = hdfsListDirectory(fs, NULL, &num);
    EXPECT_TRUE(info == NULL && EINVAL == errno);
    info = hdfsListDirectory(fs, "", &num);
    EXPECT_TRUE(info == NULL && EINVAL == errno);
    info = hdfsListDirectory(fs, BASE_DIR, NULL);
    EXPECT_TRUE(info == NULL && EINVAL == errno);
    info = hdfsListDirectory(fs, BASE_DIR"NOTEXIST", &num);
    EXPECT_TRUE(info == NULL);
    EXPECT_EQ(ENOENT, errno);
}

TEST_F(TestCInterface, TestListDirectory_Success) {
    hdfsFileInfo * info;
    int num, numFiles = 5000, numDirs = 5000;
    //empty dir
    ASSERT_EQ(0, hdfsCreateDirectory(fs, BASE_DIR"/empty"));
    info = hdfsListDirectory(fs, BASE_DIR"/empty", &num);
    ASSERT_TRUE(NULL != info);
    ASSERT_EQ(0, num);
    hdfsFreeFileInfo(info, num);
    ASSERT_EQ(0, hdfsCreateDirectory(fs, BASE_DIR"/testListDirectoryDir"));
    char path[1024];

    for (int i = 0 ; i < numFiles ; ++i) {
        sprintf(path, "%s/File%d", BASE_DIR"/testListDirectoryDir", i);
        ASSERT_TRUE(CreateFile(fs, path, 0, 0));
    }

    for (int i = 0 ; i < numDirs ; ++i) {
        sprintf(path, "%s/Dir%d", BASE_DIR"/testListDirectoryDir", i);
        ASSERT_EQ(0, hdfsCreateDirectory(fs, path));
    }

    info = hdfsListDirectory(fs, BASE_DIR "/testListDirectoryDir", &num);
    ASSERT_TRUE(info != NULL);
    EXPECT_EQ(num, numFiles + numDirs);
    hdfsFreeFileInfo(info, num);
    info = hdfsListDirectory(fs, "/", &num);
    ASSERT_TRUE(info != NULL);
    hdfsFreeFileInfo(info, num);
}

TEST_F(TestCInterface, TestGetPathInfo_InvalidInput) {
    hdfsFileInfo * info = NULL;
    //test invalid input
    info = hdfsGetPathInfo(NULL, BASE_DIR"/testGetPathInfo");
    EXPECT_TRUE(info == NULL && EINVAL == errno);
    info = hdfsGetPathInfo(fs, NULL);
    EXPECT_TRUE(info == NULL && EINVAL == errno);
    info = hdfsGetPathInfo(fs, "");
    EXPECT_TRUE(info == NULL && EINVAL == errno);
}

static std::string FileName(const std::string & path) {
    size_t pos = path.find_last_of('/');

    if (pos != path.npos && pos != path.length() - 1) {
      return path.c_str() + pos + 1;
    } else {
      return path;
    }
}

TEST_F(TestCInterface, TestGetPathInfo_Success) {
    hdfsFile file = NULL;
    hdfsFileInfo * info = NULL;
    file = hdfsOpenFile(fs, BASE_DIR"/testGetPathInfo", O_WRONLY, 0, 1, 1024);
    ASSERT_TRUE(file != NULL);
    EXPECT_EQ(0, hdfsCloseFile(fs, file));
    EXPECT_EQ(0, hdfsCreateDirectory(fs, BASE_DIR "/testGetDirInfo"));
    info = hdfsGetPathInfo(fs, BASE_DIR"/testGetPathInfo");
    ASSERT_TRUE(info != NULL);
    EXPECT_EQ(1024, info->mBlockSize);
    EXPECT_STREQ("supergroup", info->mGroup);
    EXPECT_EQ(kObjectKindFile, info->mKind);
    //EXPECT_TRUE(info->mLastAccess >= 0);
    //EXPECT_TRUE(info->mLastMod >= 0);
    //&& 0666 == info->mPermissions
    EXPECT_EQ(1, info->mReplication);
    EXPECT_EQ(0, info->mSize);
    EXPECT_STREQ("testGetPathInfo", FileName(info->mName).c_str());
    EXPECT_STREQ(USER, info->mOwner);
    hdfsFreeFileInfo(info, 1);
    info = hdfsGetPathInfo(fs, BASE_DIR "/testGetDirInfo");
    ASSERT_TRUE(info != NULL);
    EXPECT_EQ(0, info->mBlockSize);
    EXPECT_STREQ("supergroup", info->mGroup);
    EXPECT_EQ(kObjectKindDirectory, info->mKind);
    EXPECT_TRUE(info->mLastAccess >= 0);
    //EXPECT_TRUE(info->mLastMod >= 0);
    //&& 0744 == info->mPermissions
    EXPECT_EQ(0, info->mReplication);
    EXPECT_EQ(0, info->mSize);
    EXPECT_STREQ("testGetDirInfo", FileName(info->mName).c_str());
    EXPECT_STREQ(USER, info->mOwner);
    hdfsFreeFileInfo(info, 1);
    info = hdfsGetPathInfo(fs, "/");
    ASSERT_TRUE(info != NULL);
    EXPECT_TRUE(info->mKind == kObjectKindDirectory && strcmp(info->mName, "/") == 0);
    hdfsFreeFileInfo(info, 1);
}

TEST_F(TestCInterface, TestChown_InvalidInput) {
    int err = 0;
    hdfsFile file = NULL;
    file = hdfsOpenFile(fs, BASE_DIR"/testChwonFile", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    err = hdfsCloseFile(fs, file);
    EXPECT_EQ(0, err);
    //test invalid input
    err = hdfsChown(NULL, BASE_DIR"/testChwonFile", "testChown", "testChown");
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsChown(fs, NULL, "testChown", "testChown");
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsChown(fs, "", "testChown", "testChown");
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsChown(fs, BASE_DIR"/testChwonFile", NULL, NULL);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsChown(fs, BASE_DIR"/testChwonFile", "", NULL);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsChown(fs, BASE_DIR"/testChwonFile", NULL, "");
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsChown(fs, BASE_DIR"/testChwonFile", "", "");
    EXPECT_TRUE(err != 0 && EINVAL == errno);
}

TEST_F(TestCInterface, TestChown_Success) {
    int err = 0;
    hdfsFile file = NULL;
    hdfsFileInfo * info = NULL;
    //test chown on file
    file = hdfsOpenFile(fs, BASE_DIR"testChownFile", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    err = hdfsCloseFile(fs, file);
    EXPECT_EQ(0, err);
    err = hdfsChown(superfs, BASE_DIR"testChownFile", "testChown", "testChown");
    EXPECT_EQ(0, err);
    info = hdfsGetPathInfo(superfs, BASE_DIR"testChownFile");
    ASSERT_TRUE(info != NULL);
    EXPECT_TRUE(strcmp(info->mOwner, "testChown") == 0
                && strcmp(info->mGroup, "testChown") == 0);
    hdfsFreeFileInfo(info, 1);
    //test chown on directory
    err = hdfsCreateDirectory(fs, BASE_DIR"testChwonDir");
    EXPECT_EQ(0, err);
    err = hdfsChown(superfs, BASE_DIR"testChwonDir", "testChown", "testChown");
    EXPECT_EQ(0, err);
    info = hdfsGetPathInfo(superfs, BASE_DIR"testChwonDir");
    EXPECT_TRUE(info != NULL);
    EXPECT_TRUE(strcmp(info->mOwner, "testChown") == 0
                && strcmp(info->mGroup, "testChown") == 0);
    hdfsFreeFileInfo(info, 1);
}

TEST_F(TestCInterface, TestChmod_InvalidInput) {
    int err = 0;
    hdfsFile file = NULL;
    err = hdfsCreateDirectory(fs, BASE_DIR"/testChmodDir");
    EXPECT_EQ(0, err);
    file = hdfsOpenFile(fs, BASE_DIR"/testChmodFile", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    err = hdfsCloseFile(fs, file);
    EXPECT_EQ(0, err);
    //test invalid input
    err = hdfsChmod(NULL, BASE_DIR"/testChmodFile", 0777);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsChmod(fs, NULL, 0777);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsChmod(fs, "", 0777);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsChmod(fs, BASE_DIR"/testChmodFile", 02000);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
}

TEST_F(TestCInterface, TestChmod_Success) {
    int err = 0;
    hdfsFile file = NULL;
    hdfsFileInfo * info = NULL;
    err = hdfsCreateDirectory(fs, BASE_DIR"/testChmodDir");
    EXPECT_EQ(0, err);
    file = hdfsOpenFile(fs, BASE_DIR"/testChmodFile", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    err = hdfsCloseFile(fs, file);
    EXPECT_EQ(0, err);
    err = hdfsChmod(fs, BASE_DIR"/testChmodFile", 0777);
    EXPECT_EQ(0, err);
    info = hdfsGetPathInfo(fs, BASE_DIR"/testChmodFile");
    ASSERT_TRUE(info != NULL);
    //not executable
    EXPECT_TRUE(info->mPermissions == 0777);
    hdfsFreeFileInfo(info, 1);
    err = hdfsChmod(fs, BASE_DIR"/testChmodDir", 0777);
    EXPECT_EQ(0, err);
    info = hdfsGetPathInfo(fs, BASE_DIR"/testChmodDir");
    ASSERT_TRUE(info != NULL);
    EXPECT_TRUE(info->mPermissions == 0777);
    hdfsFreeFileInfo(info, 1);
}

TEST_F(TestCInterface, TestUtime_InvalidInput) {
    int err = 0;
    hdfsFile file = NULL;
    time_t now = time(NULL);
    err = hdfsCreateDirectory(fs, BASE_DIR"/testUtimeDir");
    EXPECT_EQ(0, err);
    file = hdfsOpenFile(fs, BASE_DIR"/testUtimeFile", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    err = hdfsCloseFile(fs, file);
    EXPECT_EQ(0, err);
    //test invalid input
    err = hdfsUtime(NULL, BASE_DIR"/testUtimeFile", now, now);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsUtime(fs, NULL, now, now);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsUtime(fs, "", now, now);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
}

TEST_F(TestCInterface, TestUtime_Success) {
    int err = 0;
    hdfsFile file = NULL;
    hdfsFileInfo * info = NULL;
    time_t now = time(NULL);
    err = hdfsCreateDirectory(fs, BASE_DIR"/testUtimeDir");
    EXPECT_EQ(0, err);
    file = hdfsOpenFile(fs, BASE_DIR"/testUtimeFile", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(file != NULL);
    err = hdfsCloseFile(fs, file);
    EXPECT_EQ(0, err);
    err = hdfsUtime(fs, BASE_DIR"/testUtimeDir", now, now);
    EXPECT_EQ(0, err);
    err = hdfsUtime(fs, BASE_DIR"/testUtimeFile", now, now);
    EXPECT_EQ(0, err);
    info = hdfsGetPathInfo(fs, BASE_DIR"/testUtimeFile");
    ASSERT_TRUE(info);
    EXPECT_TRUE(now / 1000 == info->mLastAccess && info->mLastAccess);
    hdfsFreeFileInfo(info, 1);
}

TEST_F(TestCInterface, TestRead_InvalidInput) {
    int err;
    char buf[10240];
    hdfsFile in = NULL, out = NULL;
    out = hdfsOpenFile(fs, BASE_DIR"/testRead", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(out != NULL);
    //test invalid input
    err = hdfsRead(NULL, in, buf, sizeof(buf));
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsRead(fs, NULL, buf, sizeof(buf));
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsRead(fs, out, buf, sizeof(buf));
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsRead(fs, in, NULL, sizeof(buf));
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsRead(fs, in, buf, 0);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    hdfsCloseFile(fs, out);
}

static void TestRead(hdfsFS fs, int readSize, int64_t blockSize,
                     int64_t fileSize) {
    int batch, done;
    size_t offset = 0;
    int64_t todo = fileSize;
    hdfsFile in = NULL;
    std::vector<char> buf(readSize + 100);
    ASSERT_TRUE(CreateFile(fs, BASE_DIR"/testRead", blockSize, fileSize));
    in = hdfsOpenFile(fs, BASE_DIR"/testRead", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(in != NULL);

    while (todo > 0) {
        batch = todo < readSize ? todo : readSize;
        done = hdfsRead(fs, in, &buf[0], batch);
        ASSERT_GT(done, 0);
        EXPECT_TRUE(Hdfs::CheckBuffer(&buf[0], done, offset));
        offset = offset + done;
        todo -= done;

        if (todo > 0) {
            batch = todo < readSize ? todo : readSize - 100;
            done = hdfsRead(fs, in, &buf[0], batch);
            ASSERT_GT(done, 0);
            EXPECT_TRUE(Hdfs::CheckBuffer(&buf[0], done, offset));
            offset = offset + done;
            todo -= done;
        }

        if (todo > 0) {
            batch = todo < readSize ? todo : readSize + 100;
            done = hdfsRead(fs, in, &buf[0], batch);
            ASSERT_GT(done, 0);
            EXPECT_TRUE(Hdfs::CheckBuffer(&buf[0], done, offset));
            offset = offset + done;
            todo -= done;
        }
    }

    EXPECT_EQ(0, hdfsRead(fs, in, &buf[0], 1));
    EXPECT_EQ(0, hdfsRead(fs, in, &buf[0], 1));
    hdfsCloseFile(fs, in);
}

TEST_F(TestCInterface, TestRead_Success) {
    TestRead(fs, 1024, 1024, 21 * 1024);
    TestRead(fs, 8 * 1024, 1024 * 1024, 21 * 1024 * 1024);
}

TEST_F(TestCInterface, TestWrite_InvalidInput) {
    int err;
    char buf[10240];
    hdfsFile in = NULL, out = NULL;
    out = hdfsOpenFile(fs, BASE_DIR"/testWrite1", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(out != NULL);
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
    out = hdfsOpenFile(fs, BASE_DIR"/testWrite", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(out != NULL);
    in = hdfsOpenFile(fs, BASE_DIR"/testWrite1", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(in != NULL);
    //test invalid input
    err = hdfsWrite(NULL, out, buf, sizeof(buf));
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsWrite(fs, NULL, buf, sizeof(buf));
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsWrite(fs, in, buf, sizeof(buf));
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsCloseFile(fs, in);
    EXPECT_EQ(0, err);
    err = hdfsWrite(fs, out, NULL, sizeof(buf));
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsWrite(fs, out, buf, 0);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
}

static void TestWrite(hdfsFS fs, int writeSize, int64_t blockSize,
                      int64_t fileSize) {
    hdfsFile out;
    size_t offset = 0;
    int64_t batch, todo = fileSize;
    std::vector<char> buffer(writeSize + 123);
    out = hdfsOpenFile(fs, BASE_DIR "/testWrite1", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(out != NULL);

    while (todo > 0) {
        batch = todo < writeSize ? todo : writeSize;
        Hdfs::FillBuffer(&buffer[0], batch, offset);
        EXPECT_EQ(batch, hdfsWrite(fs, out, &buffer[0], batch));
        todo -= batch;
        offset += batch;

        if (todo > 0) {
            batch = todo < writeSize - 123 ? todo : writeSize - 123;
            Hdfs::FillBuffer(&buffer[0], batch, offset);
            EXPECT_EQ(batch, hdfsWrite(fs, out, &buffer[0], batch));
            todo -= batch;
            offset += batch;
        }

        if (todo > 0) {
            batch = todo < writeSize + 123 ? todo : writeSize + 123;
            Hdfs::FillBuffer(&buffer[0], batch, offset);
            EXPECT_EQ(batch, hdfsWrite(fs, out, &buffer[0], batch));
            todo -= batch;
            offset += batch;
        }
    }

    EXPECT_EQ(0, hdfsCloseFile(fs, out));
    EXPECT_TRUE(CheckFileContent(fs, BASE_DIR "/testWrite1", fileSize, 0));
}

TEST_F(TestCInterface, TestWrite_Success) {
    TestWrite(fs, 512, 1024 * 1024, 21 * 1024 * 1024);
    TestWrite(fs, 654, 1024 * 1024, 21 * 1024 * 1024);
    TestWrite(fs, 64 * 1024, 1024 * 1024, 21 * 1024 * 1024);
    TestWrite(fs, 1024 * 1024, 1024 * 1024, 21 * 1024 * 1024);
}

TEST_F(TestCInterface, TestHFlush_InvalidInput) {
    int err;
    hdfsFile in = NULL, out = NULL;
    out = hdfsOpenFile(fs, BASE_DIR"/testFlush1", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(out != NULL);
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
    out = hdfsOpenFile(fs, BASE_DIR"/testFlush2", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(out != NULL);
    in = hdfsOpenFile(fs, BASE_DIR"/testFlush1", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(in != NULL);
    //test invalid input
    err = hdfsHFlush(NULL, out);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsHFlush(fs, NULL);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsHFlush(fs, in);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    EXPECT_EQ(0, hdfsCloseFile(fs, out));
    EXPECT_EQ(0, hdfsCloseFile(fs, in));
}

TEST_F(TestCInterface, TestSync_InvalidInput) {
    int err;
    hdfsFile in = NULL, out = NULL;
    out = hdfsOpenFile(fs, BASE_DIR"/testFlush1", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(out != NULL);
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
    out = hdfsOpenFile(fs, BASE_DIR"/testFlush2", O_WRONLY, 0, 0, 0);
    ASSERT_TRUE(out != NULL);
    in = hdfsOpenFile(fs, BASE_DIR"/testFlush1", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(in != NULL);
    //test invalid input
    err = hdfsSync(NULL, out);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsSync(fs, NULL);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    err = hdfsSync(fs, in);
    EXPECT_TRUE(err != 0 && EINVAL == errno);
    EXPECT_EQ(0, hdfsCloseFile(fs, out));
    EXPECT_EQ(0, hdfsCloseFile(fs, in));
}

static void TestHFlushAndSync(hdfsFS fs, hdfsFile file, const char * path, int64_t blockSize, bool sync) {
    hdfsFile in;
    size_t offset = 0;
    int64_t batch, fileSize, todo;
    fileSize = todo = blockSize * 3 + 123;
    std::vector<char> buffer;
    in = hdfsOpenFile(fs, path, O_RDONLY, 0, 0, 0);
    enum TestWriteSize {
        InChunk, ChunkBoundary, InPackage, PackageBoundary, RandomSize
    };
    TestWriteSize buferSizeTarget = InChunk;

    while (todo > 0) {
        switch (buferSizeTarget) {
        case InChunk:
            buffer.resize(123);
            buferSizeTarget = ChunkBoundary;
            break;

        case ChunkBoundary:
            buffer.resize(512 - (fileSize - todo) % 512);
            buferSizeTarget = InPackage;
            break;

        case InPackage:
            buffer.resize(512 + 123);
            buferSizeTarget = PackageBoundary;
            break;

        case PackageBoundary:
            buffer.resize(512 * 128 - (fileSize - todo) % (512 * 128));
            buferSizeTarget = RandomSize;
            break;

        default:
            buffer.resize(blockSize - 1);
            break;
        }

        batch = todo < static_cast<int>(buffer.size()) ? todo : buffer.size();
        Hdfs::FillBuffer(&buffer[0], batch, offset);
        EXPECT_EQ(batch, hdfsWrite(fs, file, &buffer[0], batch));

        if (sync) {
            EXPECT_EQ(0, hdfsSync(fs, file));
        } else {
            EXPECT_EQ(0, hdfsHFlush(fs, file));
        }

        EXPECT_TRUE(ReadFully(fs, in, &buffer[0], batch));
        EXPECT_TRUE(Hdfs::CheckBuffer(&buffer[0], batch, offset));
        todo -= batch;
        offset += batch;
    }

    hdfsCloseFile(fs, in);
    EXPECT_TRUE(CheckFileContent(fs, path, fileSize, 0));
}

TEST_F(TestCInterface, TestFlushAndSync_Success) {
    hdfsFile out;
    out = hdfsOpenFile(fs, BASE_DIR "/testFlushAndSync", O_WRONLY, 0, 0, 1024 * 1024);
    TestHFlushAndSync(fs, out, BASE_DIR "/testFlushAndSync", 1024 * 1024, false);
    EXPECT_EQ(hdfsCloseFile(fs, out), 0);
    out = hdfsOpenFile(fs, BASE_DIR "/testFlushAndSync", O_WRONLY | O_SYNC, 0, 0,
                       1024 * 1024);
    TestHFlushAndSync(fs, out, BASE_DIR "/testFlushAndSync", 1024 * 1024, false);
    EXPECT_EQ(hdfsCloseFile(fs, out), 0);
    out = hdfsOpenFile(fs, BASE_DIR "/testFlushAndSync", O_WRONLY, 0, 0, 1024 * 1024);
    TestHFlushAndSync(fs, out, BASE_DIR "/testFlushAndSync", 1024 * 1024, true);
    EXPECT_EQ(hdfsCloseFile(fs, out), 0);
    out = hdfsOpenFile(fs, BASE_DIR "/testFlushAndSync", O_WRONLY | O_SYNC, 0, 0,
                       1024 * 1024);
    TestHFlushAndSync(fs, out, BASE_DIR "/testFlushAndSync", 1024 * 1024, true);
    EXPECT_EQ(hdfsCloseFile(fs, out), 0);
}

TEST_F(TestCInterface, TestTruncate_InvalidInput) {
    int err;
    int shouldWait;
    std::vector<char> buf(10240);
    int bufferSize = buf.size();
    hdfsFile out = NULL;
    out = hdfsOpenFile(fs, BASE_DIR"/testTruncate", O_WRONLY, 0, 0, 2048);
    ASSERT_TRUE(out != NULL);
    Hdfs::FillBuffer(&buf[0], bufferSize, 0);
    EXPECT_TRUE(bufferSize == hdfsWrite(fs, out, &buf[0], bufferSize));
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
    //test invalid input
    err = hdfsTruncate(NULL, BASE_DIR"/testTruncate", 1, &shouldWait);
    EXPECT_TRUE(0 != err && EINVAL == errno);
    err = hdfsTruncate(fs, NULL, 1, &shouldWait);
    EXPECT_TRUE(0 != err && EINVAL == errno);
    err = hdfsTruncate(fs, "", 1, &shouldWait);
    EXPECT_TRUE(0 != err && EINVAL == errno);
    err = hdfsTruncate(fs, "NOTEXIST", 1, &shouldWait);
    EXPECT_TRUE(-1 == err && (ENOENT == errno || ENOTSUP == errno));
    err = hdfsTruncate(fs, BASE_DIR"/testTruncate", bufferSize + 1, &shouldWait);
    EXPECT_TRUE(0 != err && (EINVAL == errno || ENOTSUP == errno));
    err = hdfsTruncate(fs, BASE_DIR"/testTruncate", -1, &shouldWait);
    EXPECT_TRUE(0 != err && (EINVAL == errno || ENOTSUP == errno));
    out = hdfsOpenFile(fs, BASE_DIR"/testTruncate", O_WRONLY | O_APPEND, 0,
                       0, 0);
    ASSERT_TRUE(out != NULL);
    err = hdfsTruncate(fs, BASE_DIR"/testTruncate", 1, &shouldWait);
    EXPECT_TRUE(0 != err && (EBUSY == errno || ENOTSUP == errno));
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
}

TEST_F(TestCInterface, TestTruncate_Success) {
    int shouldWait;
    int err = hdfsTruncate(fs, "NOTEXIST", 1, &shouldWait);

    if (-1 == err && ENOTSUP == errno) {
        return;
    }

    size_t fileLength = 0;
    int bufferSize = 10240, blockSize = 2048;
    std::vector<char> buf(bufferSize);
    hdfsFile out = NULL;
    out = hdfsOpenFile(fs, BASE_DIR"/testTruncate", O_WRONLY, 0, 0, blockSize);
    ASSERT_TRUE(out != NULL);
    Hdfs::FillBuffer(&buf[0], buf.size(), fileLength);
    EXPECT_EQ(buf.size(), hdfsWrite(fs, out, &buf[0], buf.size()));
    fileLength += buf.size();
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
    //test truncate to the end of file
    err = hdfsTruncate(fs, BASE_DIR"/testTruncate", buf.size(), &shouldWait);
    EXPECT_EQ(0, err);
    EXPECT_FALSE(shouldWait);
    out = hdfsOpenFile(fs, BASE_DIR"/testTruncate", O_WRONLY | O_APPEND, 0, 0,
                       0);
    ASSERT_TRUE(out != NULL);
    Hdfs::FillBuffer(&buf[0], buf.size(), fileLength);
    EXPECT_EQ(buf.size(), hdfsWrite(fs, out, &buf[0], buf.size()));
    fileLength += buf.size();
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
    EXPECT_EQ(fileLength, GetFileLength(fs, BASE_DIR"/testTruncate"));
    EXPECT_TRUE(CheckFileContent(fs, BASE_DIR"/testTruncate", fileLength, 0));
    //test truncate to the block boundary
    err = hdfsTruncate(fs, BASE_DIR"/testTruncate", blockSize, &shouldWait);
    EXPECT_EQ(0, err);
    EXPECT_FALSE(shouldWait);
    EXPECT_EQ(blockSize, GetFileLength(fs, BASE_DIR"/testTruncate"));
    EXPECT_TRUE(CheckFileContent(fs, BASE_DIR"/testTruncate", blockSize, 0));
    fileLength = blockSize;
    out = hdfsOpenFile(fs, BASE_DIR"/testTruncate", O_WRONLY | O_APPEND, 0, 0,
                       0);
    ASSERT_TRUE(out != NULL);
    Hdfs::FillBuffer(&buf[0], buf.size(), fileLength);
    EXPECT_EQ(buf.size(), hdfsWrite(fs, out, &buf[0], buf.size()));
    fileLength += buf.size();
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
    EXPECT_EQ(fileLength, GetFileLength(fs, BASE_DIR"/testTruncate"));
    EXPECT_TRUE(CheckFileContent(fs, BASE_DIR"/testTruncate", fileLength, 0));
    //test truncate to 1
    err = hdfsTruncate(fs, BASE_DIR"/testTruncate", 1, &shouldWait);
    EXPECT_EQ(0, err);
    EXPECT_TRUE(shouldWait);
    fileLength = 1;

    do {
        out = hdfsOpenFile(fs, BASE_DIR "/testTruncate", O_WRONLY | O_APPEND, 0,
                           0, 0);
    } while (out == NULL && errno == EBUSY);

    ASSERT_TRUE(out != NULL);
    EXPECT_EQ(fileLength, GetFileLength(fs, BASE_DIR"/testTruncate"));
    EXPECT_TRUE(CheckFileContent(fs, BASE_DIR"/testTruncate", 1, 0));
    Hdfs::FillBuffer(&buf[0], buf.size() - 1, fileLength);
    EXPECT_EQ(buf.size() - 1,  hdfsWrite(fs, out, &buf[0], buf.size() - 1));
    fileLength += buf.size() - 1;
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
    EXPECT_EQ(fileLength, GetFileLength(fs, BASE_DIR"/testTruncate"));
    EXPECT_TRUE(CheckFileContent(fs, BASE_DIR"/testTruncate", fileLength, 0));
    //test truncate to 0
    err = hdfsTruncate(fs, BASE_DIR"/testTruncate", 0, &shouldWait);
    EXPECT_EQ(0, err);
    EXPECT_FALSE(shouldWait);
    fileLength = 0;
    EXPECT_EQ(fileLength, GetFileLength(fs, BASE_DIR"/testTruncate"));
    out = hdfsOpenFile(fs, BASE_DIR"/testTruncate", O_WRONLY | O_APPEND, 0, 0,
                       0);
    ASSERT_TRUE(out != NULL);
    Hdfs::FillBuffer(&buf[0], buf.size(), fileLength);
    EXPECT_EQ(buf.size(), hdfsWrite(fs, out, &buf[0], buf.size()));
    fileLength += buf.size();
    err = hdfsCloseFile(fs, out);
    EXPECT_EQ(0, err);
    EXPECT_EQ(fileLength, GetFileLength(fs, BASE_DIR"/testTruncate"));
    EXPECT_TRUE(CheckFileContent(fs, BASE_DIR"/testTruncate", fileLength, 0));
}

static void TestAppend(hdfsFS fs, int64_t blockSize) {
    int batch;
    hdfsFile out = NULL;
    size_t offset = 0;
    const int chunkSize = 512;
    int64_t fileLength = 0;
    std::vector<char> buffer(8 * 1024 + 123);
    //create a new empty file
    out = hdfsOpenFile(fs, BASE_DIR"/testAppend", O_WRONLY, 0, 0, blockSize);
    EXPECT_EQ(0, hdfsCloseFile(fs, out));

    for (int i = 0; i < 21; ++i) {
        //append to a new empty block
        out = hdfsOpenFile(fs, BASE_DIR"/testAppend", O_WRONLY | O_APPEND | O_SYNC, 0, 0,
                           blockSize);
        ASSERT_TRUE(out != NULL);
        //write half chunk
        Hdfs::FillBuffer(&buffer[0], chunkSize / 2, offset);
        EXPECT_EQ(chunkSize / 2, hdfsWrite(fs, out, &buffer[0], chunkSize / 2));
        offset += chunkSize / 2;
        fileLength += chunkSize / 2;
        ASSERT_EQ(0, hdfsCloseFile(fs, out));
        //append another half chunk
        out = hdfsOpenFile(fs, BASE_DIR"/testAppend", O_WRONLY | O_APPEND | O_SYNC, 0, 0,
                           blockSize);
        ASSERT_TRUE(out != NULL);
        Hdfs::FillBuffer(&buffer[0], chunkSize / 2, offset);
        EXPECT_EQ(chunkSize / 2, hdfsWrite(fs, out, &buffer[0], chunkSize / 2));
        offset += chunkSize / 2;
        fileLength += chunkSize / 2;
        ASSERT_EQ(0, hdfsCloseFile(fs, out));
        //append to a full block
        int64_t todo = blockSize - chunkSize;
        out = hdfsOpenFile(fs, BASE_DIR"/testAppend", O_WRONLY | O_APPEND, 0, 0,
                           blockSize);
        ASSERT_TRUE(out != NULL);

        while (todo > 0) {
            batch = todo < static_cast<int>(buffer.size()) ?
                    todo : buffer.size();
            Hdfs::FillBuffer(&buffer[0], batch, offset);
            ASSERT_EQ(batch, hdfsWrite(fs, out, &buffer[0], batch));
            todo -= batch;
            offset += batch;
            fileLength += batch;
        }

        ASSERT_EQ(0, hdfsCloseFile(fs, out));
    }

    EXPECT_TRUE(CheckFileContent(fs, BASE_DIR"/testAppend", fileLength, 0));
}

TEST_F(TestCInterface, TestAppend_Success) {
    TestAppend(fs, 32 * 1024);
    TestAppend(fs, 1024 * 1024);
}

TEST_F(TestCInterface, TestAppend2) {
    hdfsFile out = NULL;
    std::vector<char> buf(10240);
    LOG(INFO, "==TestAppend2==");

    for (int i = 0; i < 100; i++) {
        LOG(INFO, "Loop in testAppend2 %d", i);
        out = hdfsOpenFile(fs, BASE_DIR"/testAppend2", O_WRONLY | O_APPEND, 0, 0, 0);
        ASSERT_TRUE(NULL != out);
        Hdfs::FillBuffer(&buf[0], buf.size(), 0);
        ASSERT_EQ(buf.size(), hdfsWrite(fs, out, &buf[0], buf.size()));
        ASSERT_EQ(0, hdfsCloseFile(fs, out));
    }

    LOG(INFO, "==TestAppend2 done==");
}

TEST_F(TestCInterface, TestOpenForReadWrite) {
    hdfsFile out = NULL, in = NULL;
    out = hdfsOpenFile(fs, BASE_DIR"/TestOpenForRead", O_WRONLY | O_APPEND, 0, 0, 0);
    ASSERT_TRUE(NULL != out);
    ASSERT_FALSE(hdfsFileIsOpenForRead(out));
    ASSERT_TRUE(hdfsFileIsOpenForWrite(out));
    ASSERT_EQ(0, hdfsCloseFile(fs, out));
    in = hdfsOpenFile(fs, BASE_DIR"/TestOpenForRead", O_RDONLY, 0, 0, 0);
    ASSERT_TRUE(NULL != in);
    ASSERT_TRUE(hdfsFileIsOpenForRead(in));
    ASSERT_FALSE(hdfsFileIsOpenForWrite(in));
    ASSERT_EQ(0, hdfsCloseFile(fs, in));
    ASSERT_FALSE(hdfsFileIsOpenForRead(NULL));
    ASSERT_FALSE(hdfsFileIsOpenForWrite(NULL));
}

TEST_F(TestCInterface, TestGetHANamenode) {
    int size;
    Namenode * namenodes = NULL;
    EXPECT_TRUE(NULL == hdfsGetHANamenodes(NULL, &size));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHANamenodes("", &size));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHANamenodes("phdcluster", NULL));
    EXPECT_TRUE(errno == EINVAL);
    ASSERT_TRUE(NULL != (namenodes = hdfsGetHANamenodes("phdcluster", &size)));
    ASSERT_EQ(2, size);
    EXPECT_STREQ("mdw:8020", namenodes[0].rpc_addr);
    EXPECT_STREQ("mdw:50070", namenodes[0].http_addr);
    EXPECT_STREQ("smdw:8020", namenodes[1].rpc_addr);
    EXPECT_STREQ("smdw:50070", namenodes[1].http_addr);
    EXPECT_NO_THROW(hdfsFreeNamenodeInformation(namenodes, size));
}

TEST_F(TestCInterface, TestGetBlockFileLocations_Failure) {
    int size;
    EXPECT_TRUE(NULL == hdfsGetFileBlockLocations(NULL, NULL, 0, 0, NULL));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetFileBlockLocations(fs, NULL, 0, 0, NULL));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetFileBlockLocations(fs, "", 0, 0, NULL));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetFileBlockLocations(fs, "NOTEXIST", 0, 0, NULL));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetFileBlockLocations(fs, "NOTEXIST", 0, 1, NULL));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetFileBlockLocations(fs, "NOTEXIST", 0, 1, &size));
    EXPECT_TRUE(errno == ENOENT);
}


TEST_F(TestCInterface, TestGetBlockFileLocations_Success) {
    int size;
    BlockLocation * bl;
    hdfsFile out = NULL;
    std::vector<char> buffer(1025);
    out = hdfsOpenFile(fs, BASE_DIR"/TestGetBlockFileLocations_Failure", O_WRONLY, 0, 0, 1024);
    ASSERT_TRUE(NULL != out);
    hdfsCloseFile(fs, out);
    EXPECT_TRUE(NULL != (bl = hdfsGetFileBlockLocations(fs, BASE_DIR"/TestGetBlockFileLocations_Failure", 0, 1, &size)));
    EXPECT_EQ(0, size);
    hdfsFreeFileBlockLocations(bl, size);
    out = hdfsOpenFile(fs, BASE_DIR"/TestGetBlockFileLocations_Failure", O_WRONLY, 0, 0, 1024);
    ASSERT_TRUE(NULL != out);
    ASSERT_TRUE(buffer.size() == hdfsWrite(fs, out, &buffer[0], buffer.size()));
    ASSERT_TRUE(0 == hdfsSync(fs, out));
    EXPECT_TRUE(NULL != (bl = hdfsGetFileBlockLocations(fs, BASE_DIR"/TestGetBlockFileLocations_Failure", 0, buffer.size(), &size)));
    EXPECT_EQ(2, size);
    EXPECT_FALSE(bl[0].corrupt);
    EXPECT_FALSE(bl[1].corrupt);
    EXPECT_EQ(0, bl[0].offset);
    EXPECT_EQ(1024, bl[1].offset);
    EXPECT_EQ(1024, bl[0].length);
    hdfsFreeFileBlockLocations(bl, size);
    EXPECT_TRUE(NULL != (bl = hdfsGetFileBlockLocations(fs, BASE_DIR"/TestGetBlockFileLocations_Failure", buffer.size(), 1, &size)));
    EXPECT_EQ(0, size);
    hdfsFreeFileBlockLocations(bl, size);
    hdfsCloseFile(fs, out);
}

TEST_F(TestCInterface, TestGetHosts_Failure) {
    EXPECT_TRUE(NULL == hdfsGetHosts(NULL, NULL, 0, 0));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHosts(fs, NULL, 0, 0));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHosts(fs, "", 0, 0));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHosts(fs, "NOTEXIST", 0, 0));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHosts(fs, "NOTEXIST", -1, 1));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHosts(fs, "NOTEXIST", 0, 1));
    EXPECT_TRUE(errno == ENOENT);
}

TEST_F(TestCInterface, TestGetHosts_Success) {
    char ***hosts;
    hdfsFile out = NULL;
    std::vector<char> buffer(1025);
    out = hdfsOpenFile(fs, BASE_DIR "/TestGetHosts_Success", O_WRONLY, 0, 0,
                       1024);
    ASSERT_TRUE(NULL != out);
    hdfsCloseFile(fs, out);
    hosts = hdfsGetHosts(fs, BASE_DIR "/TestGetHosts_Success", 0, 1);
    EXPECT_TRUE(NULL != hosts);
    EXPECT_TRUE(NULL == hosts[0]);
    hdfsFreeHosts(hosts);
    out = hdfsOpenFile(fs, BASE_DIR "/TestGetHosts_Success", O_WRONLY, 0, 0,
                       1024);
    ASSERT_TRUE(NULL != out);
    ASSERT_TRUE(buffer.size() == hdfsWrite(fs, out, &buffer[0], buffer.size()));
    ASSERT_TRUE(0 == hdfsSync(fs, out));
    hosts =
        hdfsGetHosts(fs, BASE_DIR "/TestGetHosts_Success", 0, buffer.size());
    EXPECT_TRUE(NULL != hosts);
    EXPECT_TRUE(NULL != hosts[0]);
    EXPECT_TRUE(NULL != hosts[1]);
    EXPECT_TRUE(NULL == hosts[2]);
    hdfsFreeHosts(hosts);
    hdfsCloseFile(fs, out);
}
