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
package org.apache.heron.streamlet.scala.common

import org.apache.heron.streamlet.Context
import org.apache.heron.streamlet.scala.SerializableTransformer

/**
  * Test Increment SerializableTransformer
  */
class TestIncrementSerializableTransformer(factor: Int)
    extends SerializableTransformer[Int, Int] {
  override def setup(context: Context): Unit = {}

  override def transform(i: Int, f: Int => Unit): Unit = f(i + factor)

  override def cleanup(): Unit = {}
}
