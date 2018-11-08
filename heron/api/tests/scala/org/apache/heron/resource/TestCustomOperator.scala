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

package org.apache.heron.resource

import org.apache.heron.streamlet.impl.operators.{CustomOperator, CustomOperatorOutput}

class TestCustomOperator extends CustomOperator[Double, Double] {
  override def process(data: Double): CustomOperatorOutput[Double] = {
    // Success if data is positive
    if (data > 0) {
      if (data <= 100) {
        // Emit to next component if data is <= 100
        CustomOperatorOutput.succeed(data)
      } else {
        // Ignore data if it is greater than 100
        CustomOperatorOutput.succeed[Double]()
      }
    } else {
      // Error if it is 0 or negative
      CustomOperatorOutput.fail[Double]()
    }
  }
}
