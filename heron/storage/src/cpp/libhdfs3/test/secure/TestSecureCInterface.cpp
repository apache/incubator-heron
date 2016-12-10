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
#include "client/KerberosName.h"
#include "Exception.h"
#include "ExceptionInternal.h"
#include "TestUtil.h"

#include <krb5/krb5.h>

#ifndef TEST_HDFS_PREFIX
#define TEST_HDFS_PREFIX "./"
#endif

#define BASE_DIR TEST_HDFS_PREFIX"/testSecureHACInterface/"

using namespace Hdfs;
using namespace Hdfs::Internal;

static std::string ExtractPrincipalFromTicketCache(
    const std::string & cachePath) {
    krb5_context cxt = NULL;
    krb5_ccache ccache = NULL;
    krb5_principal principal = NULL;
    krb5_error_code ec = 0;
    std::string errmsg, retval;
    char * priName = NULL;

    if (!cachePath.empty()) {
        if (0 != setenv("KRB5CCNAME", cachePath.c_str(), 1)) {
            THROW(HdfsIOException, "Cannot set env parameter \"KRB5CCNAME\"");
        }
    }

    do {
        if (0 != (ec = krb5_init_context(&cxt))) {
            break;
        }

        if (0 != (ec = krb5_cc_default(cxt, &ccache))) {
            break;
        }

        if (0 != (ec = krb5_cc_get_principal(cxt, ccache, &principal))) {
            break;
        }

        if (0 != (ec = krb5_unparse_name(cxt, principal, &priName))) {
            break;
        }
    } while (0);

    if (!ec) {
        retval = priName;
    } else {
        if (cxt) {
            errmsg = krb5_get_error_message(cxt, ec);
        } else {
            errmsg = "Cannot initialize kerberos context";
        }
    }

    if (priName != NULL) {
        krb5_free_unparsed_name(cxt, priName);
    }

    if (principal != NULL) {
        krb5_free_principal(cxt, principal);
    }

    if (ccache != NULL) {
        krb5_cc_close(cxt, ccache);
    }

    if (cxt != NULL) {
        krb5_free_context(cxt);
    }

    if (!errmsg.empty()) {
        THROW(HdfsIOException,
              "FileSystem: Filed to extract principal from ticket cache: %s",
              errmsg.c_str());
    }

    return retval;
}

class TestKerberosConnectC: public ::testing::Test {
public:
    TestKerberosConnectC() {
        setenv("LIBHDFS3_CONF", "function-secure.xml", 1);
        struct hdfsBuilder * bld = hdfsNewBuilder();
        assert(bld != NULL);
        hdfsBuilderSetNameNode(bld, "default");
        hdfsBuilderSetForceNewInstance(bld);
        std::stringstream ss;
        ss.imbue(std::locale::classic());
        ss << "/tmp/krb5cc_";
        ss << getuid();
        const char * userCCpath = GetEnv("LIBHDFS3_TEST_USER_CCPATH", ss.str().c_str());
        hdfsBuilderSetKerbTicketCachePath(bld, userCCpath);
        fs = hdfsBuilderConnect(bld);
        hdfsFreeBuilder(bld);

        if (fs == NULL) {
            throw std::runtime_error("cannot connect hdfs");
        }

        if (0 != hdfsCreateDirectory(fs, BASE_DIR)) {
            throw std::runtime_error("cannot create test directory");
        }

        name = KerberosName(ExtractPrincipalFromTicketCache(userCCpath));
    }

    ~TestKerberosConnectC() {
        hdfsDelete(fs, BASE_DIR, true);
        hdfsDisconnect(fs);
    }

protected:
    hdfsFS fs;
    KerberosName name;
};

TEST_F(TestKerberosConnectC, GetDelegationToken_Failure) {
    ASSERT_TRUE(NULL == hdfsGetDelegationToken(NULL, NULL));
    ASSERT_EQ(EINVAL, errno);
    ASSERT_TRUE(NULL == hdfsGetDelegationToken(fs, NULL));
    ASSERT_EQ(EINVAL, errno);
    ASSERT_TRUE(NULL == hdfsGetDelegationToken(fs, ""));
    ASSERT_EQ(EINVAL, errno);
}

TEST_F(TestKerberosConnectC, DelegationToken) {
    char * token = NULL;
    hdfsFreeDelegationToken(NULL);
    ASSERT_EQ(-1, hdfsRenewDelegationToken(NULL, NULL));
    ASSERT_EQ(EINVAL, errno);
    ASSERT_EQ(-1, hdfsRenewDelegationToken(fs, NULL));
    ASSERT_EQ(EINVAL, errno);
    ASSERT_TRUE(NULL != (token = hdfsGetDelegationToken(fs, "Unknown")));
    ASSERT_EQ(-1, hdfsRenewDelegationToken(fs, token));
    ASSERT_EQ(EACCES, errno);
    hdfsCancelDelegationToken(fs, token);
    hdfsFreeDelegationToken(token);
    ASSERT_TRUE(NULL != (token = hdfsGetDelegationToken(fs, name.getName().c_str())));
    ASSERT_GT(hdfsRenewDelegationToken(fs, token), 0);
    ASSERT_EQ(0, hdfsCancelDelegationToken(fs, token));
    ASSERT_EQ(-1, hdfsRenewDelegationToken(fs, token));
    ASSERT_EQ(EPERM, errno);
    ASSERT_EQ(-1, hdfsCancelDelegationToken(fs, token));
    ASSERT_EQ(EPERM, errno);
    hdfsFreeDelegationToken(token);
}
