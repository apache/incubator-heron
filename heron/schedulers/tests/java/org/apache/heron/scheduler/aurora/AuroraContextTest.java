/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.scheduler.aurora;

import org.junit.Assert;
import org.junit.Test;

import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;

public class AuroraContextTest {

  @Test
  public void testUsesConfigString() {
    final String auroraTemplate = "/dir/test.aurora";
    Config config = Config.newBuilder()
        .put(AuroraContext.JOB_TEMPLATE, auroraTemplate)
        .put(Key.HERON_CONF, "/test")
        .build();
    Assert.assertEquals("Expected to use value from JOB_TEMPLATE config",
        auroraTemplate, AuroraContext.getHeronAuroraPath(config));
  }

  @Test
  public void testFallback() {
    Config config = Config.newBuilder()
        .put(Key.HERON_CONF, "/test")
        .build();
    Assert.assertEquals("Expected to use heron_conf/heron.aurora", "/test/heron.aurora",
        AuroraContext.getHeronAuroraPath(config));
  }
}
