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
package org.apache.heron.streamlet.scala

/**
 * This class contains a few standard reduces that can be used with
 * Streamlet reduce functions such as reduceByKeyAndWindow.
 * Example, assuming s is a Stringlet<T> object and each tuple has these functions:
 *   - Integer getKey() and
 *   - Double getValue()
 * To get streams of sum, min and max of all values upto the current one:
 *   s.reduceByKey(T::getKey, T::getValue, StreamletReducers::sum);
 *   s.reduceByKey(T::getKey, T::getValue, StreamletReducers::min);
 *   s.reduceByKey(T::getKey, T::getValue, StreamletReducers::max);
 */
object StreamletReducers {

  def sum(a: Int, b: Int): Int = a + b
  def sum(a: Long, b: Long): Long = a + b
  def sum(a: Float, b: Float): Float = a + b
  def sum(a: Double, b: Double): Double = a + b

  def max(a: Int, b: Int): Int = math.max(a, b)
  def max(a: Long, b: Long): Long = math.max(a, b)
  def max(a: Float, b: Float): Float = math.max(a, b)
  def max(a: Double, b: Double): Double = math.max(a, b)

  def min(a: Int, b: Int): Int = math.min(a, b)
  def min(a: Long, b: Long): Long = math.min(a, b)
  def min(a: Float, b: Float): Float = math.min(a, b)
  def min(a: Double, b: Double): Double = math.min(a, b)
}
