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
package org.apache.heron.streamlet.scala.common

import java.{io, util}
import java.util.function.Supplier

import org.apache.heron.api.state.State
import org.apache.heron.streamlet.Context

/**
  * State represents the state interface as seen by stateful bolts and spouts.
  * In Heron, state gives a notional Key/Value interface along with the
  * ability to iterate over the key/values. This class aims its test implementation.
  */
private[scala] class TestState[K <: io.Serializable, V <: io.Serializable]
  extends util.HashMap[K, V] with State[K, V] {}

/**
  * Context is the information available at runtime for operators like transform.
  * This class aims its test implementation with initial values.
  */
private[scala] class TestContext extends Context {
  override def getTaskId = 0

  override def getConfig = new util.HashMap[String, AnyRef]()

  override def getStreamName = ""

  override def getStreamPartition = 0

  override def registerMetric[T](metricName: String,
                                 collectionInterval: Int,
                                 metricFn: Supplier[T]): Unit = {}

  override def getState = new TestState[io.Serializable, io.Serializable]()

}

