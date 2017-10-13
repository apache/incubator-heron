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
 * tranformation wants to operate at a different parallelism, one can repartition the
 * Streamlet before doing the transformation.
 */
public abstract class BaseStreamlet<R> implements Streamlet<R> {
  private static final Logger LOG = Logger.getLogger(BaseStreamlet.class.getName());
  protected String name;
  protected int nPartitions;
  private List<BaseStreamlet<?>> children;
  private boolean built;

  public boolean isBuilt() {
    return built;
  }

  boolean allBuilt() {
    if (!built) {
      return false;
    }
    for (BaseStreamlet<?> child : children) {
      if (!child.allBuilt()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Sets the name of the Streamlet.
   * @param sName The name given by the user for this streamlet
   * @return Returns back the Streamlet with changed name
  */
  @Override
  public Streamlet<R> setName(String sName) {
    if (sName == null || sName.isEmpty()) {
      throw new IllegalArgumentException("Streamlet name cannot be null/empty");
    }
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

  /**
   * Sets the number of partitions of the streamlet
   * @param numPartitions The user assigned number of partitions
   * @return Returns back the Streamlet with changed number of partitions
   */
  @Override
  public Streamlet<R> setNumPartitions(int numPartitions) {
    if (numPartitions < 1) {
      throw new IllegalArgumentException("Streamlet's partitions cannot be < 1");
    }
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

  /**
   * Gets all the children of this streamlet.
   * Children of a streamlet are streamlets that are resulting from transformations of elements of
   * this and potentially other streamlets.
   * @return The kid streamlets
   */
  public List<BaseStreamlet<?>> getChildren() {
    return children;
  }

  /**
   * Create a Streamlet based on the supplier function
   * @param supplier The Supplier function to generate the elements
   */
  static <T> BaseStreamlet<T> createSupplierStreamlet(SerializableSupplier<T> supplier) {
    return new SupplierStreamlet<T>(supplier);
  }

  /**
   * Create a Streamlet based on the generator function
   * @param generator The Generator function to generate the elements
   */
  static <T> BaseStreamlet<T> createGeneratorStreamlet(Source<T> generator) {
    return new SourceStreamlet<T>(generator);
  }

  /**
   * Return a new Streamlet by applying mapFn to each element of this Streamlet
   * @param mapFn The Map Function that should be applied to each element
  */
  @Override
  public <T> Streamlet<T> map(SerializableFunction<? super R, ? extends T> mapFn) {
    MapStreamlet<R, T> retval = new MapStreamlet<>(this, mapFn);
    addChild(retval);
    return retval;
  }

  /**
   * Return a new KVStreamlet by applying mapFn to each element of this Streamlet.
   * This differs from the above map transformation in that it returns a KVStreamlet
   * instead of a plain Streamlet.
   * @param mapFn The Map function that should be applied to each element
  */
  @Override
  public <K, V> KVStreamlet<K, V> mapToKV(SerializableFunction<? super R,
                                                               ? extends KeyValue<K, V>> mapFn) {
    KVMapStreamlet<R, K, V> retval = new KVMapStreamlet<>(this, mapFn);
    addChild(retval);
    return retval;
  }

  /**
   * Return a new Streamlet by applying flatMapFn to each element of this Streamlet and
   * flattening the result
   * @param flatMapFn The FlatMap Function that should be applied to each element
   */
  @Override
  public <T> Streamlet<T> flatMap(
      SerializableFunction<? super R, ? extends Iterable<? extends T>> flatMapFn) {
    FlatMapStreamlet<R, T> retval = new FlatMapStreamlet<>(this, flatMapFn);
    addChild(retval);
    return retval;
  }

  /**
   * Return a new KVStreamlet by applying map_function to each element of this Streamlet
   * and flattening the result. It differs from the above flatMap in that it returns a
   * KVStreamlet instead of a plain Streamlet
   * @param flatMapFn The FlatMap Function that should be applied to each element
  */
  @Override
  public <K, V> KVStreamlet<K, V> flatMapToKV(SerializableFunction<? super R,
      ? extends Iterable<KeyValue<K, V>>> flatMapFn) {
    KVFlatMapStreamlet<R, K, V> retval = new KVFlatMapStreamlet<>(this, flatMapFn);
    addChild(retval);
    return retval;
  }

  /**
   * Return a new Streamlet by applying the filterFn on each element of this streamlet
   * and including only those elements that satisfy the filterFn
   * @param filterFn The filter Function that should be applied to each element
  */
  @Override
  public Streamlet<R> filter(SerializablePredicate<? super R> filterFn) {
    FilterStreamlet<R> retval = new FilterStreamlet<>(this, filterFn);
    addChild(retval);
    return retval;
  }

  /**
   * Same as filter(Identity).setNumPartitions(nPartitions)
  */
  @Override
  public Streamlet<R> repartition(int numPartitions) {
    return this.map((a) -> a).setNumPartitions(numPartitions);
  }

  /**
   * A more generalized version of repartition where a user can determine which partitions
   * any particular tuple should go to
   */
  @Override
  public Streamlet<R> repartition(int numPartitions,
                           SerializableBiFunction<? super R, Integer, List<Integer>> partitionFn) {
    RemapStreamlet<R> retval = new RemapStreamlet<>(this, partitionFn);
    retval.setNumPartitions(numPartitions);
    addChild(retval);
    return retval;
  }

  /**
   * Clones the current Streamlet. It returns an array of numClones Streamlets where each
   * Streamlet contains all the tuples of the current Streamlet
   * @param numClones The number of clones to clone
   */
  @Override
  public List<Streamlet<R>> clone(int numClones) {
    List<Streamlet<R>> retval = new ArrayList<>();
    for (int i = 0; i < numClones; ++i) {
      retval.add(repartition(getNumPartitions()));
    }
    return retval;
  }

  /**
   * Returns a new Streamlet by accumulating tuples of this streamlet over a WindowConfig
   * windowConfig and applying reduceFn on those tuples
   * @param windowConfig This is a specification of what kind of windowing strategy you like
   * to have. Typical windowing strategies are sliding windows and tumbling windows
   * @param reduceFn The reduceFn to apply over the tuples accumulated on a window
   */
  @Override
  public KVStreamlet<Window, R> reduceByWindow(WindowConfig windowConfig,
                                               SerializableBinaryOperator<R> reduceFn) {
    ReduceByWindowStreamlet<R> retval = new ReduceByWindowStreamlet<>(this,
                                                                      windowConfig, reduceFn);
    addChild(retval);
    return retval;
  }

  /**
   * Returns a new Streamlet thats the union of this and the ‘other’ streamlet. Essentially
   * the new streamlet will contain tuples belonging to both Streamlets
  */
  @Override
  public Streamlet<R> union(Streamlet<? extends R> other) {
    BaseStreamlet<? extends R> joinee = (BaseStreamlet<? extends R>) other;
    UnionStreamlet<R> retval = new UnionStreamlet<>(this, joinee);
    addChild(retval);
    joinee.addChild(retval);
    return retval;
  }

  /**
   * Logs every element of the streamlet using String.valueOf function
   * Note that LogStreamlet is an empty streamlet. That is its a streamlet
   * that does not contain any tuple. Thus this function returns void.
   */
  @Override
  public void log() {
    LogStreamlet<R> logger = new LogStreamlet<>(this);
    addChild(logger);
    return;
  }

  /**
   * Applies the consumer function for every element of this streamlet
   * @param consumer The user supplied consumer function that is invoked for each element
   */
  @Override
  public void consume(SerializableConsumer<R> consumer) {
    ConsumerStreamlet<R> consumerStreamlet = new ConsumerStreamlet<>(this, consumer);
    addChild(consumerStreamlet);
    return;
  }

  /**
   * Uses the sink to consume every element of this streamlet
   * @param sink The Sink that consumes
   */
  @Override
  public void toSink(Sink<R> sink) {
    SinkStreamlet<R> sinkStreamlet = new SinkStreamlet<>(this, sink);
    addChild(sinkStreamlet);
    return;
  }

  /**
   * Returns a  new Streamlet by applying the transformFunction on each element of this streamlet.
   * Before starting to cycle the transformFunction over the Streamlet, the open function is called.
   * This allows the transform Function to do any kind of initialization/loading, etc.
   * @param serializableTransformer The transformation function to be applied
   * @param <T> The return type of the transform
   * @return Streamlet containing the output of the transformFunction
   */
  @Override
  public <T> Streamlet<T> transform(
      SerializableTransformer<? super R, ? extends T> serializableTransformer) {
    TransformStreamlet<R, T> transformStreamlet =
        new TransformStreamlet<>(this, serializableTransformer);
    addChild(transformStreamlet);
    return transformStreamlet;
  }

  /**
   * Only used by the implementors
   */
  protected BaseStreamlet() {
    this.nPartitions = -1;
    this.children = new LinkedList<>();
    this.built = false;
  }

  public void build(TopologyBuilder bldr, Set<String> stageNames) {
    if (built) {
      throw new RuntimeException("Logic Error While building " + getName());
    }
    if (doBuild(bldr, stageNames)) {
      built = true;
      for (BaseStreamlet<?> streamlet : children) {
        streamlet.build(bldr, stageNames);
      }
    }
  }

  // This is the main interface that every Streamlet implementation should implement
  // The main tasks are generally to make sure that appropriate names/partitions are
  // computed and add a spout/bolt to the TopologyBuilder
  protected abstract boolean doBuild(TopologyBuilder bldr, Set<String> stageNames);

  public  <T> void addChild(BaseStreamlet<T> child) {
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
