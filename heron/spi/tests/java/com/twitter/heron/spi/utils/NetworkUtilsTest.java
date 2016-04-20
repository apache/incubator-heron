// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.twitter.heron.spi.utils;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketException;

import org.junit.Assert;
import org.junit.Test;

import com.twitter.heron.proto.system.Common;

public class NetworkUtilsTest {

    @Test
    public void testFreePort() {
        int numAttempts = 100;
        // Randomized test
        for (int i = 0; i < numAttempts; ++i) {
            int port = NetworkUtils.getFreePort();
            // verify that port is free
            try {
                new ServerSocket(port).close();
            } catch (SocketException se) {
                Assert.assertTrue("Returned port is not open", false);
            } catch (IOException e) {
            }
        }
    }

    @Test
    public void testGetHeronStatus() {
        Common.Status okStatus = Common.Status.newBuilder().
                setStatus(Common.StatusCode.OK)
                .build();
        Assert.assertEquals(okStatus, NetworkUtils.getHeronStatus(true));

        Common.Status notOKStatus = Common.Status.newBuilder()
                .setStatus(Common.StatusCode.NOTOK)
                .build();
        Assert.assertEquals(notOKStatus, NetworkUtils.getHeronStatus(false));

    }
}
