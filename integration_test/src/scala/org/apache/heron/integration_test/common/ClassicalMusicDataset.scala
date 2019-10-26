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
package org.apache.heron.integration_test.common

case class ClassicalMusic(composer: String,
                          title: String,
                          year: Int,
                          keyword: String)

/**
  * Common Dataset to be used by Scala Streamlet Integration Tests
  */
object ClassicalMusicDataset {

  val firstClassicalMusicList = List(
    ClassicalMusic("Bach", "Bourrée In E Minor", 1717, "guitar"),
    ClassicalMusic("Vivaldi", "Four Seasons: Winter", 1723, "rousing"),
    ClassicalMusic("Bach", "Air On The G String", 1723, "light"),
    ClassicalMusic("Mozart", "Symphony No. 40: I", 1788, "seductive"),
    ClassicalMusic("Beethoven", "Symphony No. 9: Ode To Joy", 1824, "joyful"),
    ClassicalMusic("Bizet", "Carmen: Habanera", 1875, "seductive")
  )

  val secondClassicalMusicList = List(
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

}
