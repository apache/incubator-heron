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

TEST(TestGetHAANamenodes, TestInvalidInput) {
    int size;
    EXPECT_TRUE(NULL == hdfsGetHANamenodesWithConfig(NULL, "phdcluster", &size));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHANamenodesWithConfig("", "phdcluster", &size));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHANamenodesWithConfig("invalidha.xml", "phdcluster", &size));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHANamenodesWithConfig("notExist", "phdcluster", &size));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHANamenodesWithConfig("validha.xml", NULL, &size));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHANamenodesWithConfig("validha.xml", "", &size));
    EXPECT_TRUE(errno == EINVAL);
    EXPECT_TRUE(NULL == hdfsGetHANamenodesWithConfig("validha.xml", "phdcluster", NULL));
    EXPECT_TRUE(errno == EINVAL);
}

TEST(TestGetHAANamenodes, GetHANamenodes) {
    int size;
    Namenode * namenodes = NULL;
    ASSERT_TRUE(NULL != (namenodes = hdfsGetHANamenodesWithConfig("validha.xml", "phdcluster", &size)));
    ASSERT_EQ(2, size);
    EXPECT_STREQ("mdw:8020", namenodes[0].rpc_addr);
    EXPECT_STREQ("mdw:50070", namenodes[0].http_addr);
    EXPECT_STREQ("smdw:8020", namenodes[1].rpc_addr);
    EXPECT_EQ((char *)NULL, namenodes[1].http_addr);
    EXPECT_NO_THROW(hdfsFreeNamenodeInformation(namenodes, size));
}
