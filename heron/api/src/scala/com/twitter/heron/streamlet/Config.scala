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
//  Copyright 2017 Twitter. All rights reserved.
package com.twitter.heron.streamlet.scala
import java.io.Serializable
import com.twitter.heron.streamlet.impl.KryoSerializer
import Config._
import scala.beans.{BeanProperty, BooleanBeanProperty}


object Config {

  private val MB: Long = 1024 * 1024

  private val GB: Long = 1024 * MB

  object DeliverySemantics extends Enumeration {

    val ATMOST_ONCE: DeliverySemantics = new DeliverySemantics()

    val ATLEAST_ONCE: DeliverySemantics = new DeliverySemantics()

    val EFFECTIVELY_ONCE: DeliverySemantics = new DeliverySemantics()

    class DeliverySemantics extends Val

    implicit def convertValue(v: Value): DeliverySemantics =
      v.asInstanceOf[DeliverySemantics]

  }

  object Serializer extends Enumeration {

    val JAVA: Serializer = new Serializer()

    val KRYO: Serializer = new Serializer()

    class Serializer extends Val

    implicit def convertValue(v: Value): Serializer =
      v.asInstanceOf[Serializer]

  }

  object Defaults {

    val USE_KRYO: Boolean = true

    val CONFIG: com.twitter.heron.api.Config =
      new com.twitter.heron.api.Config()

    val CPU: Float = 1.0f

    val RAM: Long = 100 * MB

    val SEMANTICS: DeliverySemantics = DeliverySemantics.ATMOST_ONCE

    val SERIALIZER: Serializer = Serializer.KRYO

  }

  /**
    * Sets the topology to use the default configuration: 100 megabytes of RAM per container, 1.0
    * CPUs per container, at-most-once delivery semantics, and the Kryo serializer.
    */
  def defaultConfig(): Config = new Builder().build()

  /**
    * Returns a new {@link Builder} that can be used to create a configuration object for Streamlet
    * API topologies
    */
  def newBuilder(): Builder = new Builder()

  private def translateSemantics(semantics: DeliverySemantics)
  : com.twitter.heron.api.Config.TopologyReliabilityMode = semantics match {
    case ATMOST_ONCE =>
      com.twitter.heron.api.Config.TopologyReliabilityMode.ATMOST_ONCE
    case ATLEAST_ONCE =>
      com.twitter.heron.api.Config.TopologyReliabilityMode.ATLEAST_ONCE
    case EFFECTIVELY_ONCE =>
      com.twitter.heron.api.Config.TopologyReliabilityMode.EFFECTIVELY_ONCE
    case _ => com.twitter.heron.api.Config.TopologyReliabilityMode.ATMOST_ONCE

  }

  class Builder private () {

    private var config: com.twitter.heron.api.Config = Defaults.CONFIG

    private var cpu: Float = Defaults.CPU

    private var ram: Long = Defaults.RAM

    private var deliverySemantics: DeliverySemantics = Defaults.SEMANTICS

    private var serializer: Serializer = Serializer.KRYO

    /**
      * Sets the per-container (per-instance) CPU to be used by this topology
      * @param perContainerCpu Per-container (per-instance) CPU as a float
      */
    def setPerContainerCpu(perContainerCpu: Float): Builder = {
      this.cpu = perContainerCpu
      this
    }

    /**
      * Sets the per-container (per-instance) RAM to be used by this topology
      * @param perContainerRam Per-container (per-instance) RAM expressed as a Long.
      */
    def setPerContainerRam(perContainerRam: Long): Builder = {
      this.ram = perContainerRam
      this
    }

    /**
      * Sets the per-container (per-instance) RAM to be used by this topology as a number of bytes
      * @param perContainerRam Per-container (per-instance) RAM expressed as a Long.
      */
    def setPerContainerRamInBytes(perContainerRam: Long): Builder = {
      this.ram = perContainerRam
      this
    }

    /**
      * Sets the per-container (per-instance) RAM to be used by this topology in megabytes
      * @param perContainerRamMB Per-container (per-instance) RAM expressed as a Long.
      */
    def setPerContainerRamInMegabytes(perContainerRamMB: Long): Builder = {
      this.ram = perContainerRamMB * MB
      this
    }

    /**
      * Sets the per-container (per-instance) RAM to be used by this topology in gigabytes
      * @param perContainerRamGB Per-container (per-instance) RAM expressed as a Long.
      */
    def setPerContainerRamInGigabytes(perContainerRamGB: Long): Builder = {
      this.ram = perContainerRamGB * GB
      this
    }

    /**
      * Sets the number of containers to run this topology
      * @param numContainers The number of containers across which to distribute this topology
      */
    def setNumContainers(numContainers: Int): Builder = {
      config.setNumStmgrs(numContainers)
      this
    }

    /**
      * Sets the delivery semantics of the topology
      * @param semantics The delivery semantic to be enforced
      */
    def setDeliverySemantics(semantics: DeliverySemantics): Builder = {
      this.deliverySemantics = semantics
      config.setTopologyReliabilityMode(Config.translateSemantics(semantics))
      this
    }

    /**
      * Sets some user-defined key/value mapping
      * @param key The user-defined key
      * @param value The user-defined value
      */
    def setUserConfig(key: String, value: AnyRef): Builder = {
      config.put(key, value)
      this
    }

    private def useKryo(): Unit = {
      config.setSerializationClassName(classOf[KryoSerializer].getName)
    }

    /**
      * Sets the {@link Serializer} to be used by the topology (current options are {@link
      * KryoSerializer} and the native Java serializer.
      * @param topologySerializer The data serializer to use for streamlet elements in the topology.
      */
    def setSerializer(topologySerializer: Serializer): Builder = {
      this.serializer = topologySerializer
      this
    }

    def build(): Config = {
      if (serializer == Serializer.KRYO) {
        useKryo()
      }
      new Config(this)
    }

  }

}

/**
  * Config is the way users configure the execution of the topology.
  * Things like streamlet delivery semantics, resources used, as well as
  * user-defined key/value pairs are passed on to the topology runner via
  * this class.
  */
@SerialVersionUID(6204498077403076352L)
class Config private (builder: Builder) extends Serializable {

  private val cpu: Float = builder.cpu

  private val ram: Long = builder.ram

  @BeanProperty
  val deliverySemantics: DeliverySemantics = builder.deliverySemantics

  @BeanProperty
  val serializer: Serializer = builder.serializer

  @BeanProperty
  var heronConfig: com.twitter.heron.api.Config = builder.config

  /**
    * Gets the CPU used per topology container
    * @return the per-container CPU as a float
    */
  def getPerContainerCpu(): Float = cpu

  /**
    * Gets the RAM used per topology container as a number of bytes
    * @return the per-container RAM in bytes
    */
  def getPerContainerRam(): Long = ram

  /**
    * Gets the RAM used per topology container as a number of gigabytes
    * @return the per-container RAM in gigabytes
    */
  def getPerContainerRamAsGigabytes(): Long = Math.round(ram.toDouble / GB)

  /**
    * Gets the RAM used per topology container as a number of megabytes
    * @return the per-container RAM in megabytes
    */
  def getPerContainerRamAsMegabytes(): Long = Math.round(ram.toDouble / MB)

  /**
    * Gets the RAM used per topology container as a number of bytes
    * @return the per-container RAM in bytes
    */
  def getPerContainerRamAsBytes(): Long = getPerContainerRam

}

