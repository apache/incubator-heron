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

package com.twitter.heron.streamlet;

import com.twitter.heron.classification.InterfaceStability;

/**
 * A BaseStreamlet is a base interface from which both Streamlet and KVStreamlet inherit.
 * It is essentially a container which has a name and a partition count.
 */
@InterfaceStability.Evolving
public interface BaseStreamlet<T> {
  /**
   * Sets the name of the BaseStreamlet.
   * @param sName The name given by the user for this BaseStreamlet
   * @return Returns back the Streamlet with changed name
  */
  T setName(String sName);

  /**
   * Gets the name of the Streamlet.
   * @return Returns the name of the Streamlet
   */
  String getName();

  /**
   * Sets the number of partitions of the streamlet
   * @param numPartitions The user assigned number of partitions
   * @return Returns back the Streamlet with changed number of partitions
   */
  T setNumPartitions(int numPartitions);

  /**
   * Gets the number of partitions of this Streamlet.
   * @return the number of partitions of this Streamlet
   */
  int getNumPartitions();
}
