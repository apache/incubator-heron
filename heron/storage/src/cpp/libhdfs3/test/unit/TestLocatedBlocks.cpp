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
#include "server/LocatedBlock.h"
#include "server/LocatedBlocks.h"

using namespace Hdfs::Internal;

TEST(TestLocatedBlocks, TestFindBlock){
    // repro GPSQL-3051
    LocatedBlocksImpl *lbs = new LocatedBlocksImpl();

    // one element in blocks
    lbs->setFileLength(10*1024+500);
    LocatedBlock *blk = new LocatedBlock(10240);
    blk->setNumBytes(500);
    lbs->getBlocks().push_back(*blk);
    const LocatedBlock *lb = lbs->findBlock(10239);
    EXPECT_TRUE(lb == NULL);
    lb = lbs->findBlock(10240);
    EXPECT_TRUE(lb->getOffset() == blk->getOffset());
    EXPECT_TRUE(lb->getNumBytes() == blk->getNumBytes());

    lbs->getBlocks().clear();
    // 3 elements in blocks
    lbs->setFileLength(1024*2+100);
    LocatedBlock *blk1 = new LocatedBlock(0);
    blk1->setNumBytes(1024);
    LocatedBlock *blk2 = new LocatedBlock(1024);
    blk2->setNumBytes(1024);
    LocatedBlock *blk3 = new LocatedBlock(2048);
    blk3->setNumBytes(100);
    lbs->getBlocks().push_back(*blk1);
    lbs->getBlocks().push_back(*blk2);
    lbs->getBlocks().push_back(*blk3);

    lb = lbs->findBlock(-100);
    EXPECT_TRUE(lb == NULL);
    lb = lbs->findBlock(0);
    EXPECT_TRUE(lb->getOffset() == 0);
    lb = lbs->findBlock(100);
    EXPECT_TRUE(lb->getOffset() == 0);
    lb = lbs->findBlock(1023);
    EXPECT_TRUE(lb->getOffset() == 0);
    lb = lbs->findBlock(1024);
    EXPECT_TRUE(lb->getOffset() == 1024);
    lb = lbs->findBlock(1024+100);
    EXPECT_TRUE(lb->getOffset() == 1024);
    lb = lbs->findBlock(2047);
    EXPECT_TRUE(lb->getOffset() == 1024);
    lb = lbs->findBlock(2047);
    EXPECT_TRUE(lb->getOffset() == 1024);
    lb = lbs->findBlock(2048);
    EXPECT_TRUE(lb->getOffset() == 2048);
    lb = lbs->findBlock(2048+100-1);
    EXPECT_TRUE(lb->getOffset() == 2048);

    lb = lbs->findBlock(2048+100);
    EXPECT_TRUE(lb == NULL);
    lb = lbs->findBlock(2048+1000);
    EXPECT_TRUE(lb == NULL);

    delete blk;
    delete blk1;
    delete blk2;
    delete blk3;
    delete lbs;
}
