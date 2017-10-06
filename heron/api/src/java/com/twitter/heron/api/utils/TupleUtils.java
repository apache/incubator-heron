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
package com.twitter.heron.api.utils;

import com.twitter.heron.api.Constants;
import com.twitter.heron.api.tuple.Tuple;

public final class TupleUtils {
  private TupleUtils() {
    // No instantiation
  }

  public static boolean isTick(Tuple tuple) {
    return tuple != null
        && Constants.SYSTEM_COMPONENT_ID.equals(tuple.getSourceComponent())
        && Constants.SYSTEM_TICK_STREAM_ID.equals(tuple.getSourceStreamId());
  }
}
