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

import org.apache.heron.api.spout.IRichSpout
import org.apache.heron.api.topology.TopologyBuilder
import org.apache.heron.streamlet.impl.{BuilderImpl => JavaBuilderImpl}

import org.apache.heron.streamlet.scala.{Builder, Source, Streamlet}
import org.apache.heron.streamlet.scala.converter.ScalaToJavaConverter._

class BuilderImpl(builder: org.apache.heron.streamlet.Builder)
    extends Builder {

  override def newSource[R](supplier: () => R): Streamlet[R] = {
    val serializableSupplier = toSerializableSupplier[R](supplier)
    val newJavaStreamlet = builder.newSource(serializableSupplier)
    StreamletImpl.fromJavaStreamlet[R](newJavaStreamlet)
  }

  override def newSource[R](generator: Source[R]): Streamlet[R] = {
    val javaSourceObj = toJavaSource[R](generator)
    val newJavaStreamlet = builder.newSource(javaSourceObj)
    StreamletImpl.fromJavaStreamlet[R](newJavaStreamlet)
  }

  override def newSource[R](spout: IRichSpout): Streamlet[R] = {
    val newJavaStreamlet = builder.newSource[R](spout)
    StreamletImpl.fromJavaStreamlet[R](newJavaStreamlet)
  }

  def build(): TopologyBuilder =
    builder.asInstanceOf[JavaBuilderImpl].build()

  def build(topologyBuilder: TopologyBuilder): TopologyBuilder =
    builder.asInstanceOf[JavaBuilderImpl].build(topologyBuilder)

}
