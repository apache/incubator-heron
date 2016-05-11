/*
 * Copyright 2016 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.twitter.heron.spouts.kafka.common;

import java.io.Serializable;

/** ID used to split kafka partition among  spout executors. Number of executors actually
 * processing a kafka stream is less than or equal to number of partitions in the stream.
 * Partitions are uniformly distributed to all spout executors.
 * Id behave as a string "host:port:partitonNumber"
 */
public class GlobalPartitionId implements Comparable<GlobalPartitionId>, Serializable {
  /** host address */
  public final String host;

  /** port address */
  public final int port;

  /** partition id */
  public final int partition;

  /** Ctor */
  public GlobalPartitionId(String host, int port, int partition) {
    this.host = host;
    this.port = port;
    this.partition = partition;
  }

  /** Ids are used inside HashMap and HashSets. So deep comparison methods */
  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    return toString().equals(other.toString());
  }

  @Override
  public int hashCode() {
    return toString().hashCode();
  }

  @Override
  public String toString() {
    return String.format("%s:%d:%d", host, port, partition);
  }

  @Override
  public int compareTo(GlobalPartitionId other) {
    return toString().compareTo(other.toString());
  }
}
