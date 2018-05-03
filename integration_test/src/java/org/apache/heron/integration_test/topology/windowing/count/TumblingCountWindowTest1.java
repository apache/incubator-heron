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

package org.apache.heron.integration_test.topology.windowing.count;

import java.net.MalformedURLException;

import org.apache.heron.api.bolt.BaseWindowedBolt;
import org.apache.heron.integration_test.topology.windowing.WindowTestBase;

public class TumblingCountWindowTest1 extends WindowTestBase {

  public TumblingCountWindowTest1(String[] args) throws MalformedURLException {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    WindowTestBase topology = new TumblingCountWindowTest1(args)
        .withSleepBetweenTuples(1000)
        .withWindowLength(BaseWindowedBolt.Count.of(10));
    topology.submit();
  }
}
