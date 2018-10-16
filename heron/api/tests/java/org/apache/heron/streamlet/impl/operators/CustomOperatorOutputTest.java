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

package org.apache.heron.streamlet.impl.operators;

import java.util.Optional;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CustomOperatorOutputTest {
  @Test
  public void testOutputSucceed() {
    Optional<CustomOperatorOutput<Integer>> output = CustomOperatorOutput.succeed();
    assertTrue(output.isPresent());
    assertTrue(output.get().getData().isEmpty());
    assertTrue(output.get().isAnchored());
  }

  @Test
  public void testOutputFail() {
    Optional<CustomOperatorOutput<Integer>> output = CustomOperatorOutput.fail();
    assertFalse(output.isPresent());
  }
}
