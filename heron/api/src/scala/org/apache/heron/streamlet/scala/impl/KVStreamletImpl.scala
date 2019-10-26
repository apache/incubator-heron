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
package org.apache.heron.streamlet.scala.impl

import org.apache.heron.streamlet.{
  KVStreamlet => JavaKVStreamlet,
  Streamlet => JavaStreamlet
}
import org.apache.heron.streamlet.scala.KVStreamlet

object KVStreamletImpl {
  def fromJavaKVStreamlet[K, V](javaKVStreamlet: JavaKVStreamlet[K, V]): KVStreamlet[K, V] =
    new KVStreamletImpl[K, V](javaKVStreamlet)

  def toJavaKVStreamlet[K, V](streamlet: KVStreamlet[K, V]): JavaKVStreamlet[K, V] =
    streamlet.asInstanceOf[KVStreamletImpl[K, V]].javaKVStreamlet
}

/**
 * This class provides Scala Streamlet Implementation by wrapping Java Streamlet API.
 * Passed User defined Scala Functions are transformed to related FunctionalInterface versions and
 * related Java Streamlet is transformed to Scala version again.
 */
class KVStreamletImpl[K, V](val javaKVStreamlet: JavaKVStreamlet[K, V])
    extends StreamletImpl(javaKVStreamlet) with KVStreamlet[K, V] {
}
