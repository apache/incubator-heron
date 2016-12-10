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
#include "client/FileSystemKey.h"
#include "client/InputStream.h"
#include "client/OutputStream.h"
#include "client/OutputStream.h"
#include "client/Permission.h"
#include "DateTime.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "gtest/gtest.h"
#include "Logger.h"
#include "Memory.h"
#include "server/NamenodeInfo.h"
#include "TestUtil.h"
#include "XmlConfig.h"

#include <ctime>

#ifndef TEST_HDFS_PREFIX
#define TEST_HDFS_PREFIX "./"
#endif

#define BASE_DIR TEST_HDFS_PREFIX"/testSecureHA/"

using namespace Hdfs;
using namespace Hdfs::Internal;

class TestKerberosConnect : public ::testing::Test {
public:
    TestKerberosConnect() : conf("function-secure.xml") {
        std::stringstream ss;
        ss.imbue(std::locale::classic());
        ss << "/tmp/krb5cc_";
        ss << getuid();
        const char * userCCpath = GetEnv("LIBHDFS3_TEST_USER_CCPATH",
                                         ss.str().c_str());
        conf.set("hadoop.security.kerberos.ticket.cache.path", userCCpath);
    }

    ~TestKerberosConnect() {
    }

protected:
    Config conf;
};

TEST_F(TestKerberosConnect, connect) {
    FileSystem fs(conf);
    ASSERT_NO_THROW(DebugException(fs.connect()));
}

TEST_F(TestKerberosConnect, GetDelegationToken_Failure) {
    FileSystem fs(conf);
    ASSERT_NO_THROW(DebugException(fs.connect()));
    EXPECT_THROW(fs.getDelegationToken(NULL), InvalidParameter);
    EXPECT_THROW(fs.getDelegationToken(""), InvalidParameter);
    fs.disconnect();
}

TEST_F(TestKerberosConnect, DelegationToken_Failure) {
    std::string token;
    FileSystem fs(conf);
    ASSERT_NO_THROW(DebugException(fs.connect()));
    EXPECT_THROW(DebugException(fs.renewDelegationToken(token)), HdfsInvalidBlockToken);
    ASSERT_NO_THROW(token = fs.getDelegationToken("Unknown"));
    EXPECT_THROW(fs.renewDelegationToken(token), AccessControlException);
    ASSERT_NO_THROW(token = fs.getDelegationToken());
    ASSERT_NO_THROW(fs.cancelDelegationToken(token));
    EXPECT_THROW(DebugException(fs.renewDelegationToken(token)), HdfsInvalidBlockToken);
    EXPECT_THROW(DebugException(fs.cancelDelegationToken(token)), HdfsInvalidBlockToken);
    fs.disconnect();
}

TEST_F(TestKerberosConnect, DelegationToken) {
    FileSystem fs(conf);
    ASSERT_NO_THROW(DebugException(fs.connect()));
    ASSERT_NO_THROW({
        std::string token = fs.getDelegationToken();
        fs.renewDelegationToken(token);
        fs.cancelDelegationToken(token);
    });
    fs.disconnect();
}

TEST(TestDeletationToken, ToFromString) {
    Token t1, t2;
    std::string str;
    ASSERT_NO_THROW(str = t1.toString());
    ASSERT_NO_THROW(t2.fromString(str));
    EXPECT_TRUE(t1 == t2);
    t1.setIdentifier("test");
    ASSERT_NO_THROW(str = t1.toString());
    ASSERT_NO_THROW(t2.fromString(str));
    EXPECT_TRUE(t1 == t2);
    t1.setPassword("test");
    ASSERT_NO_THROW(str = t1.toString());
    ASSERT_NO_THROW(t2.fromString(str));
    EXPECT_TRUE(t1 == t2);
    t1.setKind("test");
    ASSERT_NO_THROW(str = t1.toString());
    ASSERT_NO_THROW(t2.fromString(str));
    EXPECT_TRUE(t1 == t2);
    t1.setService("test");
    ASSERT_NO_THROW(str = t1.toString());
    ASSERT_NO_THROW(t2.fromString(str));
    EXPECT_TRUE(t1 == t2);
}

class TestToken: public ::testing::Test {
public:
    TestToken() {
        Config conf("function-secure.xml");
        std::stringstream ss;
        ss.imbue(std::locale::classic());
        ss << "/tmp/krb5cc_";
        ss << getuid();
        const char * userCCpath = GetEnv("LIBHDFS3_TEST_USER_CCPATH", ss.str().c_str());
        conf.set("hadoop.security.kerberos.ticket.cache.path", userCCpath);
        fs = shared_ptr<FileSystem> (new FileSystem(conf));
        fs->connect();

        try {
            fs->deletePath(BASE_DIR, true);
        } catch (...) {
        }

        fs->mkdirs(BASE_DIR, 0755);
        relengBin = GetEnv("RELENG_BIN", "./");
    }

    ~TestToken() {
        try {
            fs->disconnect();
        } catch (...) {
        }
    }

protected:
    shared_ptr<FileSystem> fs;
    std::string relengBin;
};

class TestNamenodeHA: public TestToken {
public:
    void switchToStandbyNamenode() {
        std::string cmd = relengBin + "/ha_failover.sh";
        int rc = std::system(cmd.c_str());

        if (rc) {
            LOG(WARNING, "failed to invoke ha_failover.sh, return code is %d", rc);
        }
    }

    void CheckFileContent(const std::string & path, int64_t len, size_t offset, bool switchNamenode = false) {
        InputStream in;
        EXPECT_NO_THROW(in.open(*fs, path.c_str(), true));
        std::vector<char> buff(20 * 1024);
        int rc, todo = len, batch;

        while (todo > 0) {
            if (switchNamenode && todo < len / 2) {
                switchToStandbyNamenode();
                switchNamenode = false;
            }

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
};

static void TestInputOutputStream(FileSystem & fs) {
    OutputStream out;
    ASSERT_NO_THROW(DebugException(out.open(fs, BASE_DIR "file", Create, 0644, true, 0, 1024)));
    std::vector<char> buffer(1024 * 3 + 1);
    FillBuffer(&buffer[0], buffer.size(), 0);
    ASSERT_NO_THROW(DebugException(out.append(&buffer[0], buffer.size())));
    ASSERT_NO_THROW(out.sync());
    InputStream in;
    ASSERT_NO_THROW(DebugException(in.open(fs, BASE_DIR "file")));
    memset(&buffer[0], 0, buffer.size());
    ASSERT_NO_THROW(DebugException(in.readFully(&buffer[0], buffer.size())));
    EXPECT_TRUE(CheckBuffer(&buffer[0], buffer.size(), 0));
    ASSERT_NO_THROW(in.close());
    ASSERT_NO_THROW(out.close());
}

TEST_F(TestToken, BlockToken) {
    TestInputOutputStream(*fs);
}

static void VerifyToken(const std::string & token, const std::string & host,
                        const std::string & port) {
    std::stringstream ss;
    ss.imbue(std::locale::classic());
    ss << "curl  -o /tmp/VerifyToken.out --silent --no-buffer -i \"http://"
       << host << ":" << port
       << "/webhdfs/v1/?op=GETHOMEDIRECTORY&delegation=" << token << "\""
       << std::endl;
    std::cerr << "cmd: " << ss.str();
    ASSERT_EQ(0, std::system(ss.str().c_str()));
    ASSERT_EQ(0, access("/tmp/VerifyToken.out", R_OK));
    ASSERT_EQ(0, std::system("grep -E '200 OK|StandbyException' /tmp/VerifyToken.out"));
}

static void ExtractHostPort(const std::string & uri, std::string & host,
                            std::string & port) {
    std::stringstream ss;
    ss.imbue(std::locale::classic());

    if (NULL == strstr(uri.c_str(), "://")) {
        ss << "hdfs://" << uri;
    } else {
        ss << uri;
    }

    FileSystemKey key(ss.str(), NULL);
    host = key.getHost();
    port = key.getPort();
}

TEST_F(TestToken, DelegatationToken) {
    std::string token;
    Hdfs::Internal::Token t;
    std::string host, port;
    Config conf("function-secure.xml");
    std::string namenode = conf.getString("dfs.default.uri");
    std::vector<NamenodeInfo> namenodes;
    int count = 1000;

    for (int i = 0; i < count; ++i) {
        ExtractHostPort(namenode, host, port);
        ASSERT_NO_THROW(token = fs->getDelegationToken());
        ASSERT_NO_THROW(DebugException(t.fromString(token)));

        try {
            namenodes = NamenodeInfo::GetHANamenodeInfo(host, conf);

            for (size_t i = 0; i < namenodes.size(); ++i) {
                ExtractHostPort(namenodes[i].getHttpAddr(), host, port);
                VerifyToken(token, host, port);
            }
        } catch (HdfsConfigNotFound & e) {
            VerifyToken(token, host, "50070");
        }

        ASSERT_NO_THROW(fs->cancelDelegationToken(token));
    }
}

TEST_F(TestToken, DelegatationTokenAccess) {
    std::string token;
    ASSERT_NO_THROW(token = fs->getDelegationToken());
    Config conf("function-secure.xml");
    FileSystem tempfs(conf);
    ASSERT_NO_THROW(tempfs.connect(conf.getString("dfs.default.uri"), NULL, token.c_str()));
    TestInputOutputStream(tempfs);
    ASSERT_NO_THROW(fs->cancelDelegationToken(token));
}

TEST_F(TestNamenodeHA, Throughput) {
    const char * filename = BASE_DIR"TestNamenodeHA";
    std::vector<char> buffer(64 * 1024);
    int64_t fileLength =  5 * 1024 * 1024 * 1024ll;
    int64_t todo = fileLength, batch;
    size_t offset = 0;
    OutputStream ous;
    bool switchNamenode = true;
    EXPECT_NO_THROW(DebugException(ous.open(*fs, filename, Create | Overwrite /*| SyncBlock*/)));

    while (todo > 0) {
        if (switchNamenode && todo < fileLength / 2) {
            switchToStandbyNamenode();
            switchNamenode = false;
        }

        batch = todo < static_cast<int>(buffer.size()) ? todo : buffer.size();
        Hdfs::FillBuffer(&buffer[0], batch, offset);
        ASSERT_NO_THROW(DebugException(ous.append(&buffer[0], batch)));
        todo -= batch;
        offset += batch;
    }

    ASSERT_NO_THROW(DebugException(ous.close()));
    CheckFileContent(filename, fileLength, 0, true);
}

static void CreateFile(FileSystem & fs, const std::string & path) {
    OutputStream ous;
    ASSERT_NO_THROW(DebugException(ous.open(fs, path.c_str())));
    ASSERT_NO_THROW(ous.sync());
    ASSERT_NO_THROW(ous.close());
}

static void AppendFile(FileSystem & fs, const std::string & path, int64_t len) {
    OutputStream ous;
    ous.open(fs, path.c_str(), Append | SyncBlock, 0755, true);
    size_t offset = 0;
    int64_t todo = len, batch;
    std::vector<char> buffer(32 * 1024);

    while (todo > 0) {
        batch = static_cast<int64_t>(buffer.size()) < todo ? buffer.size() : todo;
        Hdfs::FillBuffer(&buffer[0], batch, offset);
        ASSERT_NO_THROW(DebugException(ous.append(&buffer[0], batch)));
        todo -= batch;
        offset += batch;
    }

    ASSERT_NO_THROW(ous.sync());
    ASSERT_NO_THROW(ous.close());
}

TEST_F(TestNamenodeHA, SmallFiles) {
    int count = 100;
    int64_t filesize = 32 * 1024 * 1024ll;
    const char * filename = BASE_DIR"smallfile";
    std::vector<std::string> paths;
    bool switchNamenode = true;

    for (int i = 0; i < count; ++i) {
        std::stringstream ss;
        ss.imbue(std::locale::classic());
        ss << filename << i;
        paths.push_back(ss.str());
    }

    /*
     * test create
     */
    for (int i = 0; i < count; ++i) {
        if (switchNamenode && i > count / 2) {
            switchToStandbyNamenode();
            switchNamenode = false;
        }

        CreateFile(*fs, paths[i]);
    }

    /*
     * test append
     */
    switchNamenode = true;

    for (int i = 0; i < count; ++i) {
        if (switchNamenode && i > count / 2) {
            switchToStandbyNamenode();
            switchNamenode = false;
        }

        AppendFile(*fs, paths[i], filesize);
    }

    /*
     * test read
     */
    switchNamenode = true;

    for (int i = 0; i < count; ++i) {
        if (switchNamenode && i > count / 2) {
            switchToStandbyNamenode();
            switchNamenode = false;
        }

        CheckFileContent(paths[i], filesize, 0);
    }
}
