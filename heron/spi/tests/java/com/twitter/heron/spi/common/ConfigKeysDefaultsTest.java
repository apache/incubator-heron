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

package com.twitter.heron.spi.common;

import java.util.Map;
import java.util.logging.Logger;

import org.junit.Assert;
import org.junit.Test;

public class ConfigKeysDefaultsTest {
  private static final Logger LOG = Logger.getLogger(ConfigKeysDefaultsTest.class.getName());

  /*
   * Test to check if all the default keys have a corresponding config key
   */
  @Test
  public void testConfigKeysDefaults() throws Exception {
    Map<String, Object> keys = ConfigKeys.keys;
    Map<String, Object> defaults = ConfigDefaults.defaults;

    for (Map.Entry<String, Object> entry : defaults.entrySet()) {
      String key = entry.getKey();
      System.out.println(key);
      Assert.assertTrue(keys.containsKey(key));
    }
  }
}
