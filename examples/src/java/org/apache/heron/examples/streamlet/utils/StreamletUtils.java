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


package org.apache.heron.examples.streamlet.utils;

import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;

/**
 * A collection of helper functions for the Streamlet API example topologies
 */
public final class StreamletUtils {
  private StreamletUtils() {
  }

  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Fetches the topology's name from the first command-line argument or
   * throws an exception if not present.
   */
  public static String getTopologyName(String[] args) throws Exception {
    if (args.length == 0) {
      throw new Exception("You must supply a name for the topology");
    } else {
      return args[0];
    }
  }

  /**
   * Selects a random item from a list. Used in many example source streamlets.
   */
  public static <T> T randomFromList(List<T> ls) {
    return ls.get(new Random().nextInt(ls.size()));
  }

  /**
   * Fetches the topology's parallelism from the second-command-line
   * argument or defers to a supplied default.
   */
  public static int getParallelism(String[] args, int defaultParallelism) {
    return (args.length > 1) ? Integer.parseInt(args[1]) : defaultParallelism;
  }

  /**
   * Converts a list of integers into a comma-separated string.
   */
  public static String intListAsString(List<Integer> ls) {
    return String.join(", ", ls.stream().map(i -> i.toString()).collect(Collectors.toList()));
  }
}
