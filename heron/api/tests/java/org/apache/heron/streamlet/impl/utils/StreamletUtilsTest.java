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

package org.apache.heron.streamlet.impl.utils;

import org.junit.Test;

import static org.apache.heron.streamlet.impl.utils.StreamletUtils.checkNotBlank;
import static org.apache.heron.streamlet.impl.utils.StreamletUtils.checkNotNull;
import static org.apache.heron.streamlet.impl.utils.StreamletUtils.require;

public class StreamletUtilsTest {

  @Test
  public void testRequire() {
    String text = "test_text";
    require(!text.isEmpty(), "text should not be blank");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testRequireWithNegativeCase() {
    String text = "";
    require(!text.isEmpty(), "text should not be blank");
  }

  @Test
  public void testCheckNotBlank() {
    checkNotBlank("test_text", "text should not be blank");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckNotBlankWithNullReference() {
    checkNotBlank(null, "text should not be blank");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckNotBlankWithEmptyString() {
    checkNotBlank("", "text should not be blank");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testCheckNotBlankWithBlankString() {
    checkNotBlank(" ", "text should not be blank");
  }

  @Test
  public void testCheckNotNull() {
    checkNotNull(new String(), "text should not be null");
  }

  @Test(expected = NullPointerException.class)
  public void testCheckNotNullWithNullReference() {
    String text = null;
    checkNotNull(text, "text should not be null");
  }

}
