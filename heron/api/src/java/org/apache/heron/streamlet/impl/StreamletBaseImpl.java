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
package org.apache.heron.streamlet.impl;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.streamlet.StreamletBase;

import static org.apache.heron.streamlet.impl.utils.StreamletUtils.checkNotBlank;
import static org.apache.heron.streamlet.impl.utils.StreamletUtils.require;

/**
 * A Streamlet is a (potentially unbounded) ordered collection of tuples.
 * Streamlets originate from pub/sub systems(such Pulsar/Kafka), or from
 * static data(such as csv files, HDFS files), or for that matter any other
 * source. They are also created by transforming existing Streamlets using
 * operations such as map/flatMap, etc.
 * Besides the tuples, a Streamlet has the following properties associated with it
 * a) name. User assigned or system generated name to refer the streamlet
 * b) nPartitions. Number of partitions that the streamlet is composed of. Thus the
 *    ordering of the tuples in a Streamlet is wrt the tuples within a partition.
 *    This allows the system to distribute  each partition to different nodes across the cluster.
 * A bunch of transformations can be done on Streamlets(like map/flatMap, etc.). Each
 * of these transformations operate on every tuple of the Streamlet and produce a new
 * Streamlet. One can think of a transformation attaching itself to the stream and processing
 * each tuple as they go by. Thus the parallelism of any operator is implicitly determined
 * by the number of partitions of the stream that it is operating on. If a particular
 * transformation wants to operate at a different parallelism, one can repartition the
 * Streamlet before doing the transformation.
 */
public abstract class StreamletBaseImpl<R> implements StreamletBase<R> {
  private static final Logger LOG = Logger.getLogger(StreamletBaseImpl.class.getName());
  protected String name;
  protected int nPartitions;
  private List<StreamletBaseImpl<?>> children;
  private boolean built;

  /**
   * Only used by the implementors
   */
  protected StreamletBaseImpl() {
    this.name = null;
    this.nPartitions = -1;
    this.children = new LinkedList<>();
    this.built = false;
  }

  protected enum StreamletNamePrefix {
    CONSUMER("consumer"),
    COUNT("count"),
    CUSTOM("custom"),
    CUSTOM_BASIC("customBasic"),
    CUSTOM_WINDOW("customWindow"),
    FILTER("filter"),
    FLATMAP("flatmap"),
    JOIN("join"),
    KEYBY("keyBy"),
    LOGGER("logger"),
    MAP("map"),
    SOURCE("generator"),
    REDUCE("reduce"),
    REMAP("remap"),
    SINK("sink"),
    SPLIT("split"),
    SPOUT("spout"),
    SUPPLIER("supplier"),
    TRANSFORM("transform"),
    UNION("union");

    private final String prefix;

    StreamletNamePrefix(final String prefix) {
      this.prefix = prefix;
    }

    @Override
    public String toString() {
      return prefix;
    }
  }

  /**
   * Sets the name of the Streamlet.
   * @param sName The name given by the user for this streamlet
   * @return Returns back the Streamlet with changed name
   */
  @Override
  public StreamletBase<R> setName(String sName) {
    checkNotBlank(sName, "Streamlet name cannot be null/blank");

    this.name = sName;
    return this;
  }

  /**
   * Gets the name of the Streamlet.
   * @return Returns the name of the Streamlet
   */
  @Override
  public String getName() {
    return name;
  }

  private String defaultNameCalculator(StreamletNamePrefix prefix, Set<String> stageNames) {
    int index = 1;
    String calculatedName;
    while (true) {
      calculatedName = new StringBuilder(prefix.toString()).append(index).toString();
      if (!stageNames.contains(calculatedName)) {
        break;
      }
      index++;
    }
    LOG.info("Calculated stage Name as " + calculatedName);
    return calculatedName;
  }

  /**
   * Sets a default unique name to the Streamlet by type if it is not set.
   * Otherwise, just checks its uniqueness.
   * @param prefix The name prefix of this streamlet
   * @param stageNames The collections of created streamlet/stage names
   */
  protected void setDefaultNameIfNone(StreamletNamePrefix prefix, Set<String> stageNames) {
    if (getName() == null) {
      setName(defaultNameCalculator(prefix, stageNames));
    }
    if (stageNames.contains(getName())) {
      throw new RuntimeException(String.format(
          "The stage name %s is used multiple times in the same topology", getName()));
    }
    stageNames.add(getName());
  }

  /**
   * Sets the number of partitions of the streamlet
   * @param numPartitions The user assigned number of partitions
   * @return Returns back the Streamlet with changed number of partitions
   */
  @Override
  public StreamletBase<R> setNumPartitions(int numPartitions) {
    require(numPartitions > 0, "Streamlet's partitions number should be > 0");

    this.nPartitions = numPartitions;
    return this;
  }

  /**
   * Gets the number of partitions of this Streamlet.
   * @return the number of partitions of this Streamlet
   */
  @Override
  public int getNumPartitions() {
    return nPartitions;
  }

  public <T> void addChild(StreamletBaseImpl<T> child) {
    children.add(child);
  }

  /**
   * Gets all the children of this streamlet.
   * Children of a streamlet are streamlets that are resulting from transformations of elements of
   * this and potentially other streamlets.
   * @return The kid streamlets
   */
  public List<StreamletBaseImpl<?>> getChildren() {
    return children;
  }

  public void build(TopologyBuilder bldr, Set<String> stageNames) {
    if (built) {
      throw new RuntimeException("Logic Error While building " + getName());
    }

    if (doBuild(bldr, stageNames)) {
      built = true;
      for (StreamletBaseImpl<?> streamlet : getChildren()) {
        streamlet.build(bldr, stageNames);
      }
    }
  }

  public boolean isBuilt() {
    return built;
  }

  public boolean isFullyBuilt() {
    if (!isBuilt()) {
      return false;
    }
    for (StreamletBaseImpl<?> child : children) {
      if (!child.isFullyBuilt()) {
        return false;
      }
    }
    return true;
  }

  // This is the main interface that every Streamlet implementation should implement
  // The main tasks are generally to make sure that appropriate names/partitions are
  // computed and add a spout/bolt to the TopologyBuilder
  protected abstract boolean doBuild(TopologyBuilder bldr, Set<String> stageNames);
}
