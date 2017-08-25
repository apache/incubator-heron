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

package com.twitter.heron.dsl.windowing;


/**
 * A Streamlet is a (potentially unbounded) ordered collection of tuples.
 Streamlets originate from pub/sub systems(such Pulsar/Kafka), or from static data(such as
 csv files, HDFS files), or for that matter any other source. They are also created by
 transforming existing Streamlets using operations such as map/flatMap, etc.
 Besides the tuples, a Streamlet has the following properties associated with it
 a) name. User assigned or system generated name to refer the streamlet
 b) nPartitions. Number of partitions that the streamlet is composed of. The nPartitions
 could be assigned by the user or computed by the system
 */
public final class WindowConfig {
  public static WindowConfig createTimeWindow(int windowDuration) {
    return new WindowConfig(WindowType.TIME, windowDuration, windowDuration);
  }
  public static WindowConfig createTimeWindow(int windowDuration, int slideInterval) {
    return new WindowConfig(WindowType.TIME, windowDuration, slideInterval);
  }
  public static WindowConfig createTupleWindow(int windowSize) {
    return new WindowConfig(WindowType.COUNT, windowSize, windowSize);
  }
  public static WindowConfig createTupleWindow(int windowSize, int slideSize) {
    return new WindowConfig(WindowType.COUNT, windowSize, slideSize);
  }
  private enum WindowType { TIME, COUNT }
  private WindowType windowType;
  private int windowSize;
  private int slideInterval;
  private WindowConfig(WindowType type, int windowSize, int slideInterval) {
    this.windowType = type;
    this.windowSize = windowSize;
    this.slideInterval = slideInterval;
  }
}