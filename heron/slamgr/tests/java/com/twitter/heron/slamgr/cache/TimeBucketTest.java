//  Copyright 2016 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License
package com.twitter.heron.slamgr.cache;


import java.util.Date;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TimeBucketTest {

  @Before
  public void before() {

  }

  @After
  public void after() {

  }

  @Test
  public void test() {
    int now = (int) new Date().getTime() / 1000;
    TimeBucket tb = new TimeBucket(10);

    tb.data_.offerFirst("1");
    tb.data_.offerFirst("2");
    tb.data_.offerFirst("3");

    Assert.assertEquals(tb.count(), 3);
    Assert.assertEquals(tb.overlaps(now - 1, now + 10 + 1), true);
    Assert.assertEquals(tb.aggregate(), 1.0 + 2 + 3);
  }
}
