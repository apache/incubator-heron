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
import scala.beans.{BeanProperty, BooleanBeanProperty}


/**
  * Window is a container containing information about a particular window.
  * Transformations that depend on Windowing, pass the window information
  * inside their streamlets using this container.
  */
@SerialVersionUID(5103471810104775854L)
class Window() extends Serializable {

  private var startTimeMs: Long = _

  private var endTimeMs: Long = _

  @BeanProperty
  var count: Long = _

  def this(startTimeMs: Long, endTimeMs: Long, count: Long) = {
    this()
    this.startTimeMs = startTimeMs
    this.endTimeMs = endTimeMs
    this.count = count
  }

  def getStartTime(): Long = startTimeMs

  def getEndTime(): Long = endTimeMs

  override def toString(): String =
    "{WindowStart: " + String.valueOf(startTimeMs) + " WindowEnd: " +
      String.valueOf(endTimeMs) +
      " Count: " +
      String.valueOf(count) +
      " }"

}

