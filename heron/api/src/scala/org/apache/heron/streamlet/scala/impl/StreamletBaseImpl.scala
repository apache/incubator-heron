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

import org.apache.heron.streamlet.{StreamletBase => JavaStreamletBase}
import org.apache.heron.streamlet.scala.StreamletBase

object StreamletBaseImpl {
  def fromJavaStreamletBase[R](javaStreamletBase: JavaStreamletBase[R]): StreamletBase[R] =
    new StreamletBaseImpl[R](javaStreamletBase)

  def toJavaStreamletBase[R](streamlet: StreamletBase[R]): JavaStreamletBase[R] =
    streamlet.asInstanceOf[StreamletBaseImpl[R]].javaStreamletBase
}

/**
 * This class provides Scala Streamlet Implementation by wrapping Java Streamlet API.
 * Passed User defined Scala Functions are transformed to related FunctionalInterface versions and
 * related Java Streamlet is transformed to Scala version again.
 */
class StreamletBaseImpl[R](val javaStreamletBase: JavaStreamletBase[R]) extends StreamletBase[R] {
    import StreamletBaseImpl._

    /**
    * Sets the name of the Streamlet.
    *
    * @param sName The name given by the user for this Streamlet
    * @return Returns back the Streamlet with changed name
    */
  override def setName(sName: String): StreamletBase[R] =
    fromJavaStreamletBase[R](javaStreamletBase.setName(sName))

  /**
    * Gets the name of the Streamlet.
    *
    * @return Returns the name of the Streamlet
    */
  override def getName(): String = javaStreamletBase.getName

  /**
    * Sets the number of partitions of the streamlet
    *
    * @param numPartitions The user assigned number of partitions
    * @return Returns back the Streamlet with changed number of partitions
    */
  override def setNumPartitions(numPartitions: Int): StreamletBase[R] =
    fromJavaStreamletBase[R](javaStreamletBase.setNumPartitions(numPartitions))

  /**
    * Gets the number of partitions of this Streamlet.
    *
    * @return the number of partitions of this Streamlet
    */
  override def getNumPartitions(): Int = javaStreamletBase.getNumPartitions
}
