//  Copyright 2018 Twitter. All rights reserved.
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
package com.twitter.heron.streamlet.scala.common

import java.{io, util}
import java.util.function.Supplier

import com.twitter.heron.api.state.State
import com.twitter.heron.streamlet.Context

private[scala] class TestState[K <: io.Serializable, V <: io.Serializable]
  extends util.HashMap[K, V] with State[K, V] {}

private[scala] class TestContext extends Context {
  override def getTaskId: Int = 0

  override def getConfig: util.Map[String, AnyRef] = new util.HashMap[String, AnyRef]()

  override def getStreamName: String = ""

  override def getStreamPartition: Int = 0

  override def registerMetric[T](metricName: String,
                                 collectionInterval: Int,
                                 metricFn: Supplier[T]): Unit = {}

  override def getState: State[io.Serializable, io.Serializable] =
    new TestState[io.Serializable, io.Serializable]()

}


