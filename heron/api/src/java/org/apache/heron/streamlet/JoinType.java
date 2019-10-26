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

package org.apache.heron.streamlet;

public enum JoinType {
  /**
   * Return a new KVStreamlet by inner joining 'this streamlet with ‘other’ streamlet.
   * The join is done over elements accumulated over a time window defined by TimeWindow.
   */
  INNER,
  /**
   * Return a new KVStreamlet by left joining ‘this’ streamlet with ‘other’ streamlet.
   * The join is done over elements accumulated over a time window defined by TimeWindow.
   * Because its a left join, it is guaranteed that all elements of this streamlet will show up
   * in the resulting joined streamlet.
   */
  OUTER_LEFT,
  /**
   * Return a new KVStreamlet by right joining ‘this’ streamlet with ‘other’ streamlet.
   * The join is done over elements accumulated over a time window defined by TimeWindow.
   * Because its a right join, it is guaranteed that all elements of the other streamlet will show up
   * in the resulting joined streamlet.
   */
  OUTER_RIGHT,
  /**
   * Return a new KVStreamlet by outer joining ‘this’ streamlet with ‘other’ streamlet.
   * The join is done over elements accumulated over a time window defined by TimeWindow.
   * Because its a outer join, it is guaranteed that all elements of both this streamlet and
   * 'other' streamlet will show up in the resulting joined streamlet.
   */
  OUTER;
}
