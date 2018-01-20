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
//  Copyright 2017 Twitter. All rights reserved.
package com.twitter.heron.streamlet.scala

import java.io.Serializable
import java.util.Map
import java.util.function.Supplier
import com.twitter.heron.api.state.State

/**
  * Context is the information available at runtime for operators like transform.
  * It contains basic things like config, runtime information like task,
  * the stream that it is operating on, ProcessState, etc.
  */
trait Context {

  /**
    * Fetches the task id of the current instance of the operator
    * @return the task id.
    */
  def getTaskId(): Int

  /**
    * Fetches the config of the computation
    * @return config
    */
  def getConfig(): Map[String, Any]

  /**
    * The stream name that we are operating on
    * @return the stream name that we are operating on
    */
  def getStreamName(): String

  /**
    * The partition number that we are operating on
    * @return the partition number
    */
  def getStreamPartition(): Int

  /**
    * Register a metric function. This function will be called
    * by the system every collectionInterval seconds and the resulting value
    * will be collected
    */
  def registerMetric[T](metricName: String,
                        collectionInterval: Int,
                        metricFn: Supplier[T]): Unit

  /**
    * The state where components can store any of their local state
    * @return The state interface where users can store their local state
    */
  def getState(): State[Serializable, Serializable]

}

