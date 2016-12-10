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
#include "LruMap.h"

using namespace Hdfs::Internal;

TEST(TestLruMap, TestInsertAndFind) {
    LruMap<int, int> map(3);
    map.insert(1, 1);
    map.insert(2, 2);
    map.insert(3, 3);
    map.insert(4, 4);
    int value = 0;
    EXPECT_TRUE(map.find(2, &value));
    EXPECT_TRUE(value == 2);
    EXPECT_TRUE(map.find(3, &value));
    EXPECT_TRUE(value == 3);
    EXPECT_TRUE(map.find(4, &value));
    EXPECT_TRUE(value == 4);
    EXPECT_FALSE(map.find(1, &value));
    EXPECT_TRUE(map.find(2, &value));
    EXPECT_TRUE(value == 2);
    map.insert(5, 5);
    EXPECT_FALSE(map.find(3, &value));
}

TEST(TestLruMap, TestFindAndErase) {
    LruMap<int, int> map(3);
    map.insert(1, 1);
    map.insert(2, 2);
    map.insert(3, 3);
    map.insert(4, 4);
    int value = 0;
    EXPECT_EQ(3u, map.size());
    EXPECT_TRUE(map.findAndErase(2, &value));
    EXPECT_TRUE(value == 2);
    EXPECT_EQ(2u, map.size());
}
