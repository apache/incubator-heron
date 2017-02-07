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

#include "HWCrc32c.h"
#include "SWCrc32c.h"

#include <fstream>
#include <string>
#include <iostream>
#include <sstream>

#ifndef DATA_DIR
#define DATA_DIR ""
#endif

using namespace Hdfs::Internal;
using namespace testing;

class TestChecksum: public ::testing::Test {
    virtual void SetUp() {
        in.open(DATA_DIR"checksum1.in");
        ASSERT_TRUE(!!in);
        uint32_t value;
        std::string str;

        while (getline(in, str)) {
            std::stringstream ss(str);
            ss.imbue(std::locale::classic());
            ss >> value >> str;
            cases.push_back(std::make_pair(value, str));
        }

        in.close();
        in.open(DATA_DIR"checksum2.in");
        ASSERT_TRUE(!!in);
        in >> result;

        while (getline(in, str)) {
            strs.push_back(str);
            str.clear();
        }

        in.close();
    }

    virtual void TearDown() {
        cases.clear();
    }

protected:
    std::ifstream in;
    uint32_t result;
    std::vector<std::string> strs;
    std::vector<std::pair<uint32_t, std::string> > cases;
};

TEST_F(TestChecksum, HWCrc32c) {
    HWCrc32c cs;

    if (cs.available()) {
        EXPECT_EQ(0u, cs.getValue());

        for (size_t i = 0; i < cases.size(); ++i) {
            size_t len = cases[i].second.length();
            const char * data = cases[i].second.c_str();
            std::vector<char> buffer(sizeof(uint64_t) + len);

            for (size_t j = 0; j < sizeof(uint64_t); ++j) {
                memcpy(&buffer[j], data, len);
                cs.reset();
                cs.update(&buffer[j], len);
                EXPECT_EQ(cases[i].first, cs.getValue());
            }
        }

        cs.reset();
        EXPECT_EQ(0u, cs.getValue());

        for (size_t i = 0; i < strs.size(); ++i) {
            cs.update(&strs[i][0], strs[i].length());
        }

        EXPECT_EQ(result, cs.getValue());
    } else {
        std::cout << "skip HWCrc32c checksum test on unsupported paltform." << std::endl;
    }
}

TEST_F(TestChecksum, SWCrc32c) {
    SWCrc32c cs;
    EXPECT_EQ(0u, cs.getValue());

    for (size_t i = 0; i < cases.size(); ++i) {
        size_t len = cases[i].second.length();
        const char * data = cases[i].second.c_str();
        std::vector<char> buffer(sizeof(uint64_t) + len);

        for (size_t j = 0; j < sizeof(uint64_t); ++j) {
            memcpy(&buffer[j], data, len);
            cs.reset();
            cs.update(&buffer[j], len);
            EXPECT_EQ(cases[i].first, cs.getValue());
        }
    }

    cs.reset();
    EXPECT_EQ(0u, cs.getValue());

    for (size_t i = 0; i < strs.size(); ++i) {
        cs.update(&strs[i][0], strs[i].length());
    }

    EXPECT_EQ(result, cs.getValue());
}

