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
package com.twitter.heron.integration_test.topology.windowing.count;

import java.net.MalformedURLException;

import com.twitter.heron.api.bolt.BaseWindowedBolt;
import com.twitter.heron.integration_test.topology.windowing.WindowTestBase;

public class SlidingCountWindowTest3 extends WindowTestBase {

  public SlidingCountWindowTest3(String[] args) throws MalformedURLException {
    super(args);
  }

  public static void main(String[] args) throws Exception {
    WindowTestBase topology = new SlidingCountWindowTest3(args)
        .withSleepBetweenTuples(1000)
        .withWindowLength(BaseWindowedBolt.Count.of(7))
        .withSlidingInterval(BaseWindowedBolt.Count.of(3));
    topology.submit();
  }
}
