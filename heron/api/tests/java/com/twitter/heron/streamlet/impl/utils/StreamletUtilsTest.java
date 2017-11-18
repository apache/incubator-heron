//  Copyright 2017 Twitter. All rights reserved.
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
//  limitations under the License.
package com.twitter.heron.streamlet.impl.utils;

import org.junit.Test;

public class StreamletUtilsTest {

  private static final String ERROR_MESSAGE = "0 is not bigger than 1";
  private static final String NAME = "test_name";
  @Test
  public void testRequire() {
    StreamletUtils.require(!NAME.isEmpty(), ERROR_MESSAGE);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRequireWithIncorrectRequirement() {
    StreamletUtils.require(NAME.isEmpty(), ERROR_MESSAGE);
  }
}
