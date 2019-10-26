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
package org.apache.heron.integration_test.topology.scala_streamlet_with_map_and_filter_and_union

import org.apache.heron.api.Config
import org.apache.heron.integration_test.common.{
  AbstractTestTopology,
  ClassicalMusic,
  ScalaIntegrationTestBase
}
import org.apache.heron.integration_test.common.ClassicalMusicDataset._
import org.apache.heron.integration_test.core.TestTopologyBuilder
import org.apache.heron.streamlet.Context
import org.apache.heron.streamlet.scala.{Builder, Source}

import scala.collection.mutable.Set

object ScalaStreamletWithMapAndFilterAndUnion {

  val filterSet = Set[String]()

  def main(args: Array[String]): Unit = {
    val conf = new Config
    val topology = new ScalaStreamletWithMapAndFilterAndUnion(args)
    topology.submit(conf)
  }

}

/**
  * Scala Streamlet Integration Test by covering source, map, filter and union operations.
  */
class ScalaStreamletWithMapAndFilterAndUnion(args: Array[String])
    extends AbstractTestTopology(args)
    with ScalaIntegrationTestBase {

  import ScalaStreamletWithMapAndFilterAndUnion._

  override protected def buildTopology(
      testTopologyBuilder: TestTopologyBuilder): TestTopologyBuilder = {
    val streamletBuilder = Builder.newBuilder
    val classicalMusics1 =
      streamletBuilder
        .newSource(new ClassicalMusicSource(firstClassicalMusicList))
        .setName("classical-musics")
        .map(
          classicalMusic =>
            new ClassicalMusic(classicalMusic.composer.toUpperCase(),
                               classicalMusic.title.toUpperCase(),
                               classicalMusic.year,
                               classicalMusic.keyword.toUpperCase()))
        .setName("classical-musics-with-uppercase")

    val classicalMusics2 = streamletBuilder
      .newSource(new ClassicalMusicSource(secondClassicalMusicList))
      .setName("classical-musics-2")

    val unionStreamlet = classicalMusics1
      .union(classicalMusics2)
      .setName("classical-musics-union")

    unionStreamlet
      .map[String](classicalMusic =>
        s"${classicalMusic.composer}-${classicalMusic.year}")
      .setName("classical-musics-with-composer-and-year")
      .filter(filterSet.add(_))
      .setName("filtered-classical-musics")

    build(testTopologyBuilder, streamletBuilder)
  }

}

private class ClassicalMusicSource(classicalMusics: List[ClassicalMusic])
    extends Source[ClassicalMusic] {

  var list = List[ClassicalMusic]()

  override def setup(context: Context): Unit = {
    list = classicalMusics
  }

  override def get(): Iterable[ClassicalMusic] = list

  override def cleanup(): Unit = ???
}
