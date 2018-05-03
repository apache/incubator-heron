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
package org.apache.heron.examples.streamlet.scala.common

import org.apache.commons.lang3.StringUtils

/**
  * Utility Class for Scala Streamlet Topology Examples
  */
object ScalaTopologyExampleUtils {

  /**
    * Fetches the topology's name from the first command-line argument or
    * throws an exception if not present.
    */
  def getTopologyName(args: Array[String]) = {
    require(args.length > 0, "A Topology name should be supplied")
    require(StringUtils.isNotBlank(args(0)), "Topology name must not be blank")
    args(0)
  }
}
