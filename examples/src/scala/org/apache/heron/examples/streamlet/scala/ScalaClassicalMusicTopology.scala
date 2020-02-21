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
package org.apache.heron.examples.streamlet.scala

import scala.util.Random

import org.apache.heron.examples.streamlet.scala.common.ScalaTopologyExampleUtils
import org.apache.heron.streamlet.scala.{Builder, Runner}
import org.apache.heron.streamlet.{Config, JoinType, WindowConfig}

/**
  * Classical Music Model
  */
case class ClassicalMusic(composer: String,
                          title: String,
                          year: Int,
                          keyword: String) {
  def this() = this(composer = "", title = "", year = 0, keyword = "")
}

/**
  * This is a very simple topology that shows a series of Scala Streamlet operations
  * on two source streamlets that are created in the light of Classical Music Records.
  * Classical Music Records of first source are filtered as older than 1850.
  * After then, first and second Classical Music Sources are joined by using Year field as Key
  * and Inner as Join Type. Finally, results are printed with Log Sink.
  */
object ScalaClassicalMusicTopology {

  val classicalMusics =
    List(
      ClassicalMusic("Bach", "Bourrée In E Minor", 1717, "guitar"),
      ClassicalMusic("Vivaldi", "Four Seasons: Winter", 1723, "rousing"),
      ClassicalMusic("Bach", "Air On The G String", 1723, "light"),
      ClassicalMusic("Mozart", "Symphony No. 40: I", 1788, "seductive"),
      ClassicalMusic("Beethoven", "Symphony No. 9: Ode To Joy", 1824, "joyful"),
      ClassicalMusic("Bizet", "Carmen: Habanera", 1875, "seductive")
    )

  val classicalMusics2 =
    List(
      ClassicalMusic("Handel", "Water Music: Alla Hornpipe", 1717, "formal"),
      ClassicalMusic("Vivaldi", "Four Seasons: Spring", 1723, "formal"),
      ClassicalMusic("Bach",
                     "Cantata 147: Jesu, Joy Of Man's Desiring",
                     1723,
                     "wedding"),
      ClassicalMusic("Mozart", "Piano Sonata No. 16", 1788, "piano"),
      ClassicalMusic("Beethoven", "Symphony No. 9: II", 1824, "powerful"),
      ClassicalMusic("Tchaikovsky", "Piano Concerto No. 1", 1875, "piano")
    )

  def main(args: Array[String]): Unit = {
    val builder = Builder.newBuilder()

    val musicSource2 = builder
      .newSource[ClassicalMusic](() =>
        getRandomClassicalMusic(classicalMusics2))
      .setName("classical-music-source-2")

    val musicSource = builder
      .newSource[ClassicalMusic](() => getRandomClassicalMusic(classicalMusics))
      .setName("classical-music-source-1")

    musicSource
      .filter(_.year <= 1850)
      .setName("classical-musics-older-than-1850")
      .join[Int, ClassicalMusic, String](
        musicSource2,
        (cm: ClassicalMusic) => cm.year,
        (cm: ClassicalMusic) => cm.year,
        WindowConfig.TumblingCountWindow(50),
        JoinType.INNER,
        (cm1: ClassicalMusic, cm2: ClassicalMusic) =>
          s"${cm1.composer.toUpperCase} - ${cm1.title} - ${cm2.composer.toUpperCase} - ${cm2.title}"
      )
      .setName("joined-classical-musics-by-year")
      .log()
      .setName("log")

    val config = Config.defaultConfig()

    // Fetches the topology name from the first command-line argument
    val topologyName = ScalaTopologyExampleUtils.getTopologyName(args)

    // Finally, the processing graph and configuration are passed to the Runner, which converts
    // the graph into a Heron topology that can be run in a Heron cluster.
    new Runner().run(topologyName, config, builder)
  }

  private def getRandomClassicalMusic(
      list: List[ClassicalMusic]): ClassicalMusic =
    list(Random.nextInt(list.size))

}
