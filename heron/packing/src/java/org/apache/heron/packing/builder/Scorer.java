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

package org.apache.heron.packing.builder;

import org.apache.heron.classification.InterfaceAudience;
import org.apache.heron.classification.InterfaceStability;

/**
 * Scores an  object based on some heuristic. The ordering of the object by score could be used to
 * drive algorithms that rely on preference of objects (e.g. containers) to be used for packing
 * operations.
 */
@InterfaceAudience.LimitedPrivate
@InterfaceStability.Unstable
public interface Scorer<T> {

  /**
   * Whether or not scores produced by this scorer should sort ascending or descending
   * @return true if the low scores should sort before higher scores
   */
  boolean sortAscending();

  /**
   * Return the score for a given component on a container
   *
   * @param object the object to be scored
   * @return score for container
   */
  double getScore(T object);

}
