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

#include "Exception.h"
#include "XmlConfig.h"

#ifndef DATA_DIR
#define DATA_DIR ""
#endif

using namespace Hdfs;

TEST(TestXmlConfig, TestParse) {
    EXPECT_NO_THROW({ Config conf(DATA_DIR"hdfs-default.xml"); });
}

TEST(TestXmlConfig, ParseBadFile) {
    EXPECT_THROW({ Config conf("path not exist"); }, HdfsBadConfigFoumat);
    EXPECT_THROW({ Config conf(DATA_DIR"invalid.xml"); }, HdfsBadConfigFoumat);
}

TEST(TestXmlConfig, TestGetValue) {
    Config conf(DATA_DIR"unit-config.xml");
    EXPECT_STREQ("TestString", conf.getString("TestString"));
    EXPECT_STREQ("TestString", conf.getString("NOT EXIST", "TestString"));
    EXPECT_EQ(123456, conf.getInt32("TestInt32"));
    EXPECT_EQ(123456, conf.getInt32("NOT EXIST", 123456));
    EXPECT_THROW(conf.getInt32("TestInt32Invalid"), HdfsConfigNotFound);
    EXPECT_THROW(conf.getInt32("TestInt32OverFlow"), HdfsConfigNotFound);
    EXPECT_EQ(12345678901ll, conf.getInt64("TestInt64"));
    EXPECT_EQ(12345678901ll, conf.getInt64("NOT EXIST", 12345678901ll));
    EXPECT_THROW(conf.getInt64("TestInt64Invalid"), HdfsConfigNotFound);
    EXPECT_THROW(conf.getInt64("TestInt64OverFlow"), HdfsConfigNotFound);
    EXPECT_DOUBLE_EQ(123.456, conf.getDouble("TestDouble"));
    EXPECT_DOUBLE_EQ(123.456, conf.getDouble("NOT EXIST", 123.456));
    EXPECT_THROW(conf.getDouble("TestDoubleInvalid"), HdfsConfigNotFound);
    EXPECT_THROW(conf.getDouble("TestDoubleOverflow"), HdfsConfigNotFound);
    EXPECT_THROW(conf.getDouble("TestDoubleUnderflow"), HdfsConfigNotFound);
    EXPECT_TRUE(conf.getBool("TestTrue1"));
    EXPECT_TRUE(conf.getBool("TestTrue2"));
    EXPECT_TRUE(conf.getBool("NOT EXIST", true));
    EXPECT_FALSE(conf.getBool("TestFalse1"));
    EXPECT_FALSE(conf.getBool("TestFalse2"));
    EXPECT_FALSE(conf.getBool("NOT EXIST", false));
    EXPECT_THROW(conf.getBool("TestBoolInvalid"), HdfsConfigNotFound);
}
