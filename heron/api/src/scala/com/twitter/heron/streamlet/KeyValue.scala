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
// Copyright 2016 Twitter. All rights reserved.
package com.twitter.heron.streamlet.scala

import java.io.Serializable

import KeyValue._

import scala.beans.{BeanProperty, BooleanBeanProperty}

object KeyValue {

  def create[R, T](k: R, v: T): KeyValue[R, T] = new KeyValue[R, T](k, v)

}

/**
  * Certain operations in the Streamlet API, like join/reduce, necessitate
  * the concept of key value pairs. This file defines a generic KeyValue
  * class. We make the KeyValue class serializable to allow it to be
  * serialized between components.
  */
@SerialVersionUID(-7120757965590727554L)
class KeyValue[K, V]() extends Serializable {

  @BeanProperty
  var key: K = _

  @BeanProperty
  var value: V = _

  def this(k: K, v: V) = {
    this()
    this.key = k
    this.value = v
  }

  override def toString(): String =
    "{ " + String.valueOf(key) + " : " + String.valueOf(value) +
      " }"

}
