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

package org.apache.heron.resource;

import java.util.Optional;

import org.apache.heron.streamlet.impl.operators.CustomOperator;
import org.apache.heron.streamlet.impl.operators.CustomOperatorOutput;

public class TestCustomOperator<T extends Number> extends CustomOperator<T, T> {
  @Override
  public Optional<CustomOperatorOutput<T>> process(T data) {
    // Success if data is positive
    if (data.intValue() > 0) {
      if (data.intValue() <= 100) {
        // Emit to next component if data is <= 100
        return CustomOperatorOutput.succeed(data);
      } else {
        // Ignore data if it is greater than 100
        return CustomOperatorOutput.succeed();
      }
    } else {
      // Error if it is 0 or negative
      return CustomOperatorOutput.fail();
    }
  }
}
