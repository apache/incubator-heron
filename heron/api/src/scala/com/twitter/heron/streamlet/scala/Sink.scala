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
package com.twitter.heron.streamlet.scala

import java.io.Serializable

import com.twitter.heron.streamlet.Context

/**
  * Sink is how Streamlet's end. The put method invocation consumes the tuple into say
  * external database/cache, etc. setup/cleanup is where the sink can do any one time setup work,
  * like establishing/closing connection to sources, etc.
  */
trait Sink[T] extends Serializable {

  def setup(context: Context): Unit

  def put(tuple: T): Unit

  def cleanup(): Unit

}
