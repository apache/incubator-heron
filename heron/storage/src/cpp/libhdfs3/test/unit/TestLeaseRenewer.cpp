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

#include "client/LeaseRenewer.h"
#include "DateTime.h"
#include "MockFileSystemInter.h"

using namespace Hdfs::Internal;
using namespace testing;

TEST(TestRenewer, Renew) {
    shared_ptr<MockFileSystemInter> filesystem(new MockFileSystemInter());
    LeaseRenewerImpl renewer;
    renewer.setInterval(1000);
    EXPECT_CALL(*filesystem, getClientName()).Times(2).WillRepeatedly(Return("MockFS"));
    EXPECT_CALL(*filesystem, registerOpenedOutputStream()).Times(1);
    EXPECT_CALL(*filesystem, unregisterOpenedOutputStream()).Times(1).WillOnce(Return(true));
    EXPECT_CALL(*filesystem, renewLease()).Times(AtLeast(1)).WillRepeatedly(Return(true));
    renewer.StartRenew(filesystem);
    sleep_for(seconds(2));
    renewer.StopRenew(filesystem);
}
