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
package com.twitter.heron.metricscachemgr.metriccache;


import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class TimeBucketTest {
  private static String debugFilePath =
      "/tmp/" + TimeBucketTest.class.getSimpleName() + ".debug.txt";

  private Path file = null;
  private List<String> lines = null;

  @Before
  public void before() {
    file = Paths.get(debugFilePath);
    lines = new ArrayList<>();
  }

  @After
  public void after() throws IOException {
    Files.write(file, lines, Charset.forName("UTF-8"));
  }

  private void prepareTestData(TimeBucket tb) {
    tb.data.offerFirst("1");
    tb.data.offerFirst("2");
    tb.data.offerFirst("3");

    lines.add(tb.toString());
  }

  @Test
  public void testCount() {
    TimeBucket tb = new TimeBucket(10);
    prepareTestData(tb);

    // assertion
    Assert.assertEquals(tb.count(), 3);
  }

  @Test
  public void testOverlaps() {
    int now = (int) Instant.now().getEpochSecond();
    TimeBucket tb = new TimeBucket(10);
    prepareTestData(tb);

    // assertion
    Assert.assertEquals(tb.overlaps(now - 1, now + 10 + 1), true);
  }

  @Test
  public void testAggregate() {
    TimeBucket tb = new TimeBucket(10);
    prepareTestData(tb);

    // assertion
    Assert.assertEquals(String.valueOf(tb.aggregate()), "6.0");
  }
}
