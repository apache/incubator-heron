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
 * A Streamlet is a (potentially unbounded) ordered collection of tuples.
 * The StreamletBase class contains basic information of a Streamlet
 * such as name and partition count without the connection functions
 * such as map() and filter().
 */
trait StreamletBase[R] {

  /**
    * Sets the name of the Streamlet.
    *
    * @param sName The name given by the user for this Streamlet
    * @return Returns back the Streamlet with changed name
    */
  def setName(sName: String): StreamletBase[R]

  /**
    * Gets the name of the Streamlet.
    *
    * @return Returns the name of the Streamlet
    */
  def getName: String

  /**
    * Sets the number of partitions of the streamlet
    *
    * @param numPartitions The user assigned number of partitions
    * @return Returns back the Streamlet with changed number of partitions
    */
  def setNumPartitions(numPartitions: Int): StreamletBase[R]

  /**
    * Gets the number of partitions of this Streamlet.
    *
    * @return the number of partitions of this Streamlet
    */
  def getNumPartitions: Int
}
