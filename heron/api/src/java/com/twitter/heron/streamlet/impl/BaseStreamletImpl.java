//  Copyright 2017 Twitter. All rights reserved.
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

package com.twitter.heron.streamlet.impl;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import com.twitter.heron.api.topology.TopologyBuilder;
import com.twitter.heron.streamlet.BaseStreamlet;
import com.twitter.heron.streamlet.KVStreamlet;
import com.twitter.heron.streamlet.KeyValue;
import com.twitter.heron.streamlet.SerializableBiFunction;
import com.twitter.heron.streamlet.SerializableBinaryOperator;
import com.twitter.heron.streamlet.SerializableConsumer;
import com.twitter.heron.streamlet.SerializableFunction;
import com.twitter.heron.streamlet.SerializablePredicate;
import com.twitter.heron.streamlet.SerializableSupplier;
import com.twitter.heron.streamlet.SerializableTransformer;
import com.twitter.heron.streamlet.Sink;
import com.twitter.heron.streamlet.Source;
import com.twitter.heron.streamlet.Streamlet;
import com.twitter.heron.streamlet.Window;
import com.twitter.heron.streamlet.WindowConfig;
import com.twitter.heron.streamlet.impl.streamlets.ConsumerStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.FilterStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.FlatMapStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.KVFlatMapStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.KVMapStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.LogStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.MapStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.ReduceByWindowStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.RemapStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.SinkStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.SourceStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.SupplierStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.TransformStreamlet;
import com.twitter.heron.streamlet.impl.streamlets.UnionStreamlet;

/**
 * A simple class that takes care of the basics of a streamlet(currently name and npartitions).
 */
public abstract class BaseStreamletImpl<T> implements BaseStreamlet<T> {
  private static final Logger LOG = Logger.getLogger(BaseStreamletImpl.class.getName());
  protected String name;
  protected int nPartitions;
  private List<BaseStreamletImpl<?>> children;
  private boolean built;

  public boolean isBuilt() {
    return built;
  }

  boolean allBuilt() {
    if (!built) {
      return false;
    }
    for (BaseStreamletImpl<?> child : children) {
      if (!child.allBuilt()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Gets all the children of this streamlet.
   * Children of a streamlet are streamlets that are resulting from transformations of elements of
   * this and potentially other streamlets.
   * @return The kid streamlets
   */
  public List<BaseStreamletImpl<?>> getChildren() {
    return children;
  }

  /**
   * Sets the name of the Streamlet.
   * @param sName The name given by the user for this streamlet
   * @return Returns back the Streamlet with changed name
  */
  @Override
  public T setName(String sName) {
    if (sName == null || sName.isEmpty()) {
      throw new IllegalArgumentException("Streamlet name cannot be null/empty");
    }
    this.name = sName;
    return returnThis();
  }

  /**
   * Gets the name of the Streamlet.
   * @return Returns the name of the Streamlet
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * Sets the number of partitions of the streamlet
   * @param numPartitions The user assigned number of partitions
   * @return Returns back the Streamlet with changed number of partitions
   */
  @Override
  public T setNumPartitions(int numPartitions) {
    if (numPartitions < 1) {
      throw new IllegalArgumentException("Streamlet's partitions cannot be < 1");
    }
    this.nPartitions = numPartitions;
    return returnThis();
  }

  /**
   * Gets the number of partitions of this Streamlet.
   * @return the number of partitions of this Streamlet
   */
  @Override
  public int getNumPartitions() {
    return nPartitions;
  }

  /**
   * Only used by the implementors
   */
  protected BaseStreamletImpl() {
    this.nPartitions = -1;
    this.children = new LinkedList<>();
    this.built = false;
  }

  protected abstract T returnThis();

  public void build(TopologyBuilder bldr, Set<String> stageNames) {
    if (built) {
      throw new RuntimeException("Logic Error While building " + getName());
    }
    if (doBuild(bldr, stageNames)) {
      built = true;
      for (BaseStreamletImpl<?> streamlet : children) {
        streamlet.build(bldr, stageNames);
      }
    }
  }

  // This is the main interface that every Streamlet implementation should implement
  // The main tasks are generally to make sure that appropriate names/partitions are
  // computed and add a spout/bolt to the TopologyBuilder
  protected abstract boolean doBuild(TopologyBuilder bldr, Set<String> stageNames);

  public  <T> void addChild(BaseStreamletImpl<T> child) {
    children.add(child);
  }

  protected String defaultNameCalculator(String prefix, Set<String> stageNames) {
    int index = 1;
    String calculatedName;
    while (true) {
      calculatedName = new StringBuilder(prefix).append(index).toString();
      if (!stageNames.contains(calculatedName)) {
        break;
      }
      index++;
    }
    LOG.info("Calculated stage Name as " + calculatedName);
    return calculatedName;
  }
}
