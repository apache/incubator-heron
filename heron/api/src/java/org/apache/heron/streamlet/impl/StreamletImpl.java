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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.heron.api.topology.TopologyBuilder;
import org.apache.heron.streamlet.JoinType;
import org.apache.heron.streamlet.KeyValue;
import org.apache.heron.streamlet.KeyedWindow;
import org.apache.heron.streamlet.SerializableBiFunction;
import org.apache.heron.streamlet.SerializableBinaryOperator;
import org.apache.heron.streamlet.SerializableConsumer;
import org.apache.heron.streamlet.SerializableFunction;
import org.apache.heron.streamlet.SerializablePredicate;
import org.apache.heron.streamlet.SerializableSupplier;
import org.apache.heron.streamlet.SerializableTransformer;
import org.apache.heron.streamlet.Sink;
import org.apache.heron.streamlet.Source;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.WindowConfig;
import org.apache.heron.streamlet.impl.streamlets.ConsumerStreamlet;
import org.apache.heron.streamlet.impl.streamlets.FilterStreamlet;
import org.apache.heron.streamlet.impl.streamlets.FlatMapStreamlet;
import org.apache.heron.streamlet.impl.streamlets.GeneralReduceByKeyAndWindowStreamlet;
import org.apache.heron.streamlet.impl.streamlets.JoinStreamlet;
import org.apache.heron.streamlet.impl.streamlets.LogStreamlet;
import org.apache.heron.streamlet.impl.streamlets.MapStreamlet;
import org.apache.heron.streamlet.impl.streamlets.ReduceByKeyAndWindowStreamlet;
import org.apache.heron.streamlet.impl.streamlets.RemapStreamlet;
import org.apache.heron.streamlet.impl.streamlets.SinkStreamlet;
import org.apache.heron.streamlet.impl.streamlets.SourceStreamlet;
import org.apache.heron.streamlet.impl.streamlets.SupplierStreamlet;
import org.apache.heron.streamlet.impl.streamlets.TransformStreamlet;
import org.apache.heron.streamlet.impl.streamlets.UnionStreamlet;

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
public abstract class StreamletImpl<R> implements Streamlet<R> {
  private static final Logger LOG = Logger.getLogger(StreamletImpl.class.getName());
  protected String name;
  protected int nPartitions;
  private List<StreamletImpl<?>> children;
  private boolean built;

  public boolean isBuilt() {
    return built;
  }

  public boolean allBuilt() {
    if (!built) {
      return false;
    }
    for (StreamletImpl<?> child : children) {
      if (!child.allBuilt()) {
        return false;
      }
    }
    return true;
  }

  protected enum StreamletNamePrefix {
    CONSUMER("consumer"),
    FILTER("filter"),
    FLATMAP("flatmap"),
    REDUCE("reduceByKeyAndWindow"),
    JOIN("join"),
    LOGGER("logger"),
    MAP("map"),
    REMAP("remap"),
    SINK("sink"),
    SOURCE("generator"),
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
   * Gets all the children of this streamlet.
   * Children of a streamlet are streamlets that are resulting from transformations of elements of
   * this and potentially other streamlets.
   * @return The kid streamlets
   */
  public List<StreamletImpl<?>> getChildren() {
    return children;
  }

  /**
   * Sets the name of the Streamlet.
   * @param sName The name given by the user for this streamlet
   * @return Returns back the Streamlet with changed name
   */
  @Override
  public Streamlet<R> setName(String sName) {
    require(sName != null && !sName.trim().isEmpty(),
        "Streamlet name cannot be null/blank");
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
  public Streamlet<R> setNumPartitions(int numPartitions) {
    require(numPartitions > 0,
        "Streamlet's partitions number should be > 0");
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
   * Only used by the implementors
   */
  protected StreamletImpl() {
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
      for (StreamletImpl<?> streamlet : children) {
        streamlet.build(bldr, stageNames);
      }
    }
  }

  // This is the main interface that every Streamlet implementation should implement
  // The main tasks are generally to make sure that appropriate names/partitions are
  // computed and add a spout/bolt to the TopologyBuilder
  protected abstract boolean doBuild(TopologyBuilder bldr, Set<String> stageNames);

  public <T> void addChild(StreamletImpl<T> child) {
    children.add(child);
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
   * Create a Streamlet based on the supplier function
   * @param supplier The Supplier function to generate the elements
   */
  static <T> StreamletImpl<T> createSupplierStreamlet(SerializableSupplier<T> supplier) {
    return new SupplierStreamlet<T>(supplier);
  }

  /**
   * Create a Streamlet based on the generator function
   * @param generator The Generator function to generate the elements
   */
  static <T> StreamletImpl<T> createGeneratorStreamlet(Source<T> generator) {
    return new SourceStreamlet<T>(generator);
  }

  /**
   * Return a new Streamlet by applying mapFn to each element of this Streamlet
   * @param mapFn The Map Function that should be applied to each element
  */
  @Override
  public <T> Streamlet<T> map(SerializableFunction<R, ? extends T> mapFn) {
    MapStreamlet<R, T> retval = new MapStreamlet<>(this, mapFn);
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
      SerializableFunction<R, ? extends Iterable<? extends T>> flatMapFn) {
    FlatMapStreamlet<R, T> retval = new FlatMapStreamlet<>(this, flatMapFn);
    addChild(retval);
    return retval;
  }

  /**
   * Return a new Streamlet by applying the filterFn on each element of this streamlet
   * and including only those elements that satisfy the filterFn
   * @param filterFn The filter Function that should be applied to each element
  */
  @Override
  public Streamlet<R> filter(SerializablePredicate<R> filterFn) {
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
                           SerializableBiFunction<R, Integer, List<Integer>> partitionFn) {
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
   * Return a new Streamlet by inner joining 'this streamlet with ‘other’ streamlet.
   * The join is done over elements accumulated over a time window defined by windowCfg.
   * The elements are compared using the thisKeyExtractor for this streamlet with the
   * otherKeyExtractor for the other streamlet. On each matching pair, the joinFunction is applied.
   * @param other The Streamlet that we are joining with.
   * @param thisKeyExtractor The function applied to a tuple of this streamlet to get the key
   * @param otherKeyExtractor The function applied to a tuple of the other streamlet to get the key
   * @param windowCfg This is a specification of what kind of windowing strategy you like to
   * have. Typical windowing strategies are sliding windows and tumbling windows
   * @param joinFunction The join function that needs to be applied
   */
  @Override
  public <K, S, T> Streamlet<KeyValue<KeyedWindow<K>, T>>
        join(Streamlet<S> other, SerializableFunction<R, K> thisKeyExtractor,
             SerializableFunction<S, K> otherKeyExtractor, WindowConfig windowCfg,
             SerializableBiFunction<R, S, ? extends T> joinFunction) {
    return join(other, thisKeyExtractor, otherKeyExtractor,
        windowCfg, JoinType.INNER, joinFunction);
  }

  /**
   * Return a new KVStreamlet by joining 'this streamlet with ‘other’ streamlet. The type of joining
   * is declared by the joinType parameter.
   * The join is done over elements accumulated over a time window defined by windowCfg.
   * The elements are compared using the thisKeyExtractor for this streamlet with the
   * otherKeyExtractor for the other streamlet. On each matching pair, the joinFunction is applied.
   * Types of joins {@link JoinType}
   * @param other The Streamlet that we are joining with.
   * @param thisKeyExtractor The function applied to a tuple of this streamlet to get the key
   * @param otherKeyExtractor The function applied to a tuple of the other streamlet to get the key
   * @param windowCfg This is a specification of what kind of windowing strategy you like to
   * have. Typical windowing strategies are sliding windows and tumbling windows
   * @param joinType Type of Join. Options {@link JoinType}
   * @param joinFunction The join function that needs to be applied
   */
  @Override
  public <K, S, T> Streamlet<KeyValue<KeyedWindow<K>, T>>
        join(Streamlet<S> other, SerializableFunction<R, K> thisKeyExtractor,
             SerializableFunction<S, K> otherKeyExtractor, WindowConfig windowCfg,
             JoinType joinType, SerializableBiFunction<R, S, ? extends T> joinFunction) {

    StreamletImpl<S> joinee = (StreamletImpl<S>) other;
    JoinStreamlet<K, R, S, T> retval = JoinStreamlet.createJoinStreamlet(
        this, joinee, thisKeyExtractor, otherKeyExtractor, windowCfg, joinType, joinFunction);
    addChild(retval);
    joinee.addChild(retval);
    return retval;
  }

  /**
   * Return a new Streamlet accumulating tuples of this streamlet over a Window defined by
   * windowCfg and applying reduceFn on those tuples.
   * @param keyExtractor The function applied to a tuple of this streamlet to get the key
   * @param valueExtractor The function applied to a tuple of this streamlet to extract the value
   * to be reduced on
   * @param windowCfg This is a specification of what kind of windowing strategy you like to have.
   * Typical windowing strategies are sliding windows and tumbling windows
   * @param reduceFn The reduce function that you want to apply to all the values of a key.
   */
  @Override
  public <K, V> Streamlet<KeyValue<KeyedWindow<K>, V>> reduceByKeyAndWindow(
      SerializableFunction<R, K> keyExtractor, SerializableFunction<R, V> valueExtractor,
      WindowConfig windowCfg, SerializableBinaryOperator<V> reduceFn) {
    ReduceByKeyAndWindowStreamlet<K, V, R> retval =
        new ReduceByKeyAndWindowStreamlet<>(this, keyExtractor, valueExtractor,
            windowCfg, reduceFn);
    addChild(retval);
    return retval;
  }

  /**
   * Return a new Streamlet accumulating tuples of this streamlet over a Window defined by
   * windowCfg and applying reduceFn on those tuples. For each window, the value identity is used
   * as a initial value. All the matching tuples are reduced using reduceFn starting from this
   * initial value.
   * @param keyExtractor The function applied to a tuple of this streamlet to get the key
   * @param windowCfg This is a specification of what kind of windowing strategy you like to have.
   * Typical windowing strategies are sliding windows and tumbling windows
   * @param identity The identity element is both the initial value inside the reduction window
   * and the default result if there are no elements in the window
   * @param reduceFn The reduce function takes two parameters: a partial result of the reduction
   * and the next element of the stream. It returns a new partial result.
   */
  @Override
  public <K, T> Streamlet<KeyValue<KeyedWindow<K>, T>> reduceByKeyAndWindow(
      SerializableFunction<R, K> keyExtractor, WindowConfig windowCfg,
      T identity, SerializableBiFunction<T, R, ? extends T> reduceFn) {
    GeneralReduceByKeyAndWindowStreamlet<K, R, T> retval =
        new GeneralReduceByKeyAndWindowStreamlet<>(this, keyExtractor, windowCfg,
            identity, reduceFn);
    addChild(retval);
    return retval;
  }

  /**
   * Returns a new Streamlet that is the union of this and the ‘other’ streamlet. Essentially
   * the new streamlet will contain tuples belonging to both Streamlets
  */
  @Override
  public Streamlet<R> union(Streamlet<? extends R> other) {
    StreamletImpl<? extends R> joinee = (StreamletImpl<? extends R>) other;
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
  }

  /**
   * Applies the consumer function for every element of this streamlet
   * @param consumer The user supplied consumer function that is invoked for each element
   */
  @Override
  public void consume(SerializableConsumer<R> consumer) {
    ConsumerStreamlet<R> consumerStreamlet = new ConsumerStreamlet<>(this, consumer);
    addChild(consumerStreamlet);
  }

  /**
   * Uses the sink to consume every element of this streamlet
   * @param sink The Sink that consumes
   */
  @Override
  public void toSink(Sink<R> sink) {
    SinkStreamlet<R> sinkStreamlet = new SinkStreamlet<>(this, sink);
    addChild(sinkStreamlet);
  }

  /**
   * Returns a new Streamlet by applying the transformFunction on each element of this streamlet.
   * Before starting to cycle the transformFunction over the Streamlet, the open function is called.
   * This allows the transform Function to do any kind of initialization/loading, etc.
   * @param serializableTransformer The transformation function to be applied
   * @param <T> The return type of the transform
   * @return Streamlet containing the output of the transformFunction
   */
  @Override
  public <T> Streamlet<T> transform(
      SerializableTransformer<R, ? extends T> serializableTransformer) {
    TransformStreamlet<R, T> transformStreamlet =
        new TransformStreamlet<>(this, serializableTransformer);
    addChild(transformStreamlet);
    return transformStreamlet;
  }

  /**
   * Verifies the requirement as the utility function.
   * @param requirement The requirement to verify
   * @param errorMessage The error message
   * @throws IllegalArgumentException if the requirement fails
   */
  private void require(Boolean requirement, String errorMessage) {
    if (!requirement) {
      throw new IllegalArgumentException(errorMessage);
    }
  }
}
