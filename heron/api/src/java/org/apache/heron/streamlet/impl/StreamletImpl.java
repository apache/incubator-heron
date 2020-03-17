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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.commons.lang3.StringUtils;
import org.apache.heron.api.grouping.NoneStreamGrouping;
import org.apache.heron.api.grouping.StreamGrouping;
import org.apache.heron.api.utils.Utils;
import org.apache.heron.streamlet.IStreamletOperator;
import org.apache.heron.streamlet.JoinType;
import org.apache.heron.streamlet.KVStreamlet;
import org.apache.heron.streamlet.KeyedWindow;
import org.apache.heron.streamlet.SerializableBiFunction;
import org.apache.heron.streamlet.SerializableBinaryOperator;
import org.apache.heron.streamlet.SerializableConsumer;
import org.apache.heron.streamlet.SerializableFunction;
import org.apache.heron.streamlet.SerializablePredicate;
import org.apache.heron.streamlet.SerializableTransformer;
import org.apache.heron.streamlet.Sink;
import org.apache.heron.streamlet.Streamlet;
import org.apache.heron.streamlet.StreamletBase;
import org.apache.heron.streamlet.WindowConfig;
import org.apache.heron.streamlet.impl.streamlets.ConsumerStreamlet;
import org.apache.heron.streamlet.impl.streamlets.CountByKeyAndWindowStreamlet;
import org.apache.heron.streamlet.impl.streamlets.CountByKeyStreamlet;
import org.apache.heron.streamlet.impl.streamlets.CustomStreamlet;
import org.apache.heron.streamlet.impl.streamlets.FilterStreamlet;
import org.apache.heron.streamlet.impl.streamlets.FlatMapStreamlet;
import org.apache.heron.streamlet.impl.streamlets.GeneralReduceByKeyAndWindowStreamlet;
import org.apache.heron.streamlet.impl.streamlets.GeneralReduceByKeyStreamlet;
import org.apache.heron.streamlet.impl.streamlets.JoinStreamlet;
import org.apache.heron.streamlet.impl.streamlets.KVStreamletShadow;
import org.apache.heron.streamlet.impl.streamlets.KeyByStreamlet;
import org.apache.heron.streamlet.impl.streamlets.LogStreamlet;
import org.apache.heron.streamlet.impl.streamlets.MapStreamlet;
import org.apache.heron.streamlet.impl.streamlets.ReduceByKeyAndWindowStreamlet;
import org.apache.heron.streamlet.impl.streamlets.ReduceByKeyStreamlet;
import org.apache.heron.streamlet.impl.streamlets.RemapStreamlet;
import org.apache.heron.streamlet.impl.streamlets.SinkStreamlet;
import org.apache.heron.streamlet.impl.streamlets.SplitStreamlet;
import org.apache.heron.streamlet.impl.streamlets.StreamletShadow;
import org.apache.heron.streamlet.impl.streamlets.TransformStreamlet;
import org.apache.heron.streamlet.impl.streamlets.UnionStreamlet;

import static org.apache.heron.streamlet.impl.utils.StreamletUtils.checkNotBlank;
import static org.apache.heron.streamlet.impl.utils.StreamletUtils.checkNotNull;
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
public abstract class StreamletImpl<R>
    extends StreamletBaseImpl<R> implements Streamlet<R> {
  private static final Logger LOG = Logger.getLogger(StreamletImpl.class.getName());

  /**
   * Sets the name of the Streamlet.
   * @param sName The name given by the user for this streamlet
   * @return Returns back the Streamlet with changed name
   */
  @Override
  public Streamlet<R> setName(String sName) {
    super.setName(sName);
    return this;
  }

  /**
   * Sets the number of partitions of the streamlet
   * @param numPartitions The user assigned number of partitions
   * @return Returns back the Streamlet with changed number of partitions
   */
  @Override
  public Streamlet<R> setNumPartitions(int numPartitions) {
    super.setNumPartitions(numPartitions);
    return this;
  }

  /**
   * Set the id of the stream to be used by the children nodes.
   * Usage (assuming source is a Streamlet object with two output streams: stream1 and stream2):
   *   source.withStream("stream1").filter(...).log();
   *   source.withStream("stream2").filter(...).log();
   * @param streamId The specified stream id
   * @return Returns back the Streamlet with changed stream id
   */
  @SuppressWarnings("HiddenField")
  @Override
  public Streamlet<R> withStream(String streamId) {
    checkNotBlank(streamId, "streamId can't be empty");

    Set<String> availableIds = getAvailableStreamIds();
    if (availableIds.contains(streamId)) {
      return new StreamletShadow<R>(this) {
        @Override
        public String getStreamId() {
          return streamId;
        }
      };
    } else {
      throw new RuntimeException(
          String.format("Stream id %s is not available in %s. Available ids are: %s.",
                        streamId, getName(), availableIds.toString()));
    }
  }


  /**
   * Get the available stream ids in the Streamlet. For most Streamlets,
   * there is only one internal stream id, therefore the function
   * returns a set of one single stream id.
   * @return Returns a set of one single stream id.
   */
  protected Set<String> getAvailableStreamIds() {
    HashSet<String> ids = new HashSet<String>();
    ids.add(getStreamId());
    return ids;
  }

  /**
   * Gets the stream id of this Streamlet.
   * @return the stream id of this Streamlet`
   */
  @Override
  public String getStreamId() {
    return Utils.DEFAULT_STREAM_ID;
  }

  /**
   * Only used by the implementors
   */
  protected StreamletImpl() {
    super();
  }

  /**
   * Return a new Streamlet by applying mapFn to each element of this Streamlet
   * @param mapFn The Map Function that should be applied to each element
  */
  @Override
  public <T> Streamlet<T> map(SerializableFunction<R, ? extends T> mapFn) {
    checkNotNull(mapFn, "mapFn cannot be null");

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
    checkNotNull(flatMapFn, "flatMapFn cannot be null");

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
    checkNotNull(filterFn, "filterFn cannot be null");

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
    checkNotNull(partitionFn, "partitionFn cannot be null");

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
    require(numClones > 0, "Streamlet's clone number should be > 0");
    List<Streamlet<R>> retval = new ArrayList<>(numClones);
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
   * @param otherStreamlet The Streamlet that we are joining with.
   * @param thisKeyExtractor The function applied to a tuple of this streamlet to get the key
   * @param otherKeyExtractor The function applied to a tuple of the other streamlet to get the key
   * @param windowCfg This is a specification of what kind of windowing strategy you like to
   * have. Typical windowing strategies are sliding windows and tumbling windows
   * @param joinFunction The join function that needs to be applied
   */
  @Override
  public <K, S, T> KVStreamlet<KeyedWindow<K>, T>
        join(Streamlet<S> otherStreamlet, SerializableFunction<R, K> thisKeyExtractor,
             SerializableFunction<S, K> otherKeyExtractor, WindowConfig windowCfg,
             SerializableBiFunction<R, S, ? extends T> joinFunction) {
    checkNotNull(otherStreamlet, "otherStreamlet cannot be null");
    checkNotNull(thisKeyExtractor, "thisKeyExtractor cannot be null");
    checkNotNull(otherKeyExtractor, "otherKeyExtractor cannot be null");
    checkNotNull(windowCfg, "windowCfg cannot be null");
    checkNotNull(joinFunction, "joinFunction cannot be null");

    return join(otherStreamlet, thisKeyExtractor, otherKeyExtractor,
        windowCfg, JoinType.INNER, joinFunction);
  }

  /**
   * Return a new KVStreamlet by joining 'this streamlet with ‘other’ streamlet. The type of joining
   * is declared by the joinType parameter.
   * The join is done over elements accumulated over a time window defined by windowCfg.
   * The elements are compared using the thisKeyExtractor for this streamlet with the
   * otherKeyExtractor for the other streamlet. On each matching pair, the joinFunction is applied.
   * Types of joins {@link JoinType}
   * @param otherStreamlet The Streamlet that we are joining with.
   * @param thisKeyExtractor The function applied to a tuple of this streamlet to get the key
   * @param otherKeyExtractor The function applied to a tuple of the other streamlet to get the key
   * @param windowCfg This is a specification of what kind of windowing strategy you like to
   * have. Typical windowing strategies are sliding windows and tumbling windows
   * @param joinType Type of Join. Options {@link JoinType}
   * @param joinFunction The join function that needs to be applied
   */
  @Override
  public <K, S, T> KVStreamlet<KeyedWindow<K>, T>
        join(Streamlet<S> otherStreamlet, SerializableFunction<R, K> thisKeyExtractor,
             SerializableFunction<S, K> otherKeyExtractor, WindowConfig windowCfg,
             JoinType joinType, SerializableBiFunction<R, S, ? extends T> joinFunction) {
    checkNotNull(otherStreamlet, "otherStreamlet cannot be null");
    checkNotNull(thisKeyExtractor, "thisKeyExtractor cannot be null");
    checkNotNull(otherKeyExtractor, "otherKeyExtractor cannot be null");
    checkNotNull(windowCfg, "windowCfg cannot be null");
    checkNotNull(joinType, "joinType cannot be null");
    checkNotNull(joinFunction, "joinFunction cannot be null");

    StreamletImpl<S> joinee = (StreamletImpl<S>) otherStreamlet;
    JoinStreamlet<K, R, S, T> retval = JoinStreamlet.createJoinStreamlet(
        this, joinee, thisKeyExtractor, otherKeyExtractor, windowCfg, joinType, joinFunction);
    addChild(retval);
    joinee.addChild(retval);
    return new KVStreamletShadow<KeyedWindow<K>, T>(retval);
  }

  /**
   * Return a new Streamlet accumulating tuples of this streamlet and applying reduceFn on those tuples.
   * @param keyExtractor The function applied to a tuple of this streamlet to get the key
   * @param valueExtractor The function applied to a tuple of this streamlet to extract the value
   * to be reduced on
   * @param reduceFn The reduce function that you want to apply to all the values of a key.
   */
  @Override
  public <K, T> KVStreamlet<K, T> reduceByKey(SerializableFunction<R, K> keyExtractor,
                                              SerializableFunction<R, T> valueExtractor,
                                              SerializableBinaryOperator<T> reduceFn) {
    checkNotNull(keyExtractor, "keyExtractor cannot be null");
    checkNotNull(valueExtractor, "valueExtractor cannot be null");
    checkNotNull(reduceFn, "reduceFn cannot be null");

    ReduceByKeyStreamlet<R, K, T> retval =
        new ReduceByKeyStreamlet<>(this, keyExtractor, valueExtractor, reduceFn);
    addChild(retval);
    return new KVStreamletShadow<K, T>(retval);
  }

  /**
   * Return a new Streamlet accumulating tuples of this streamlet and applying reduceFn on those tuples.
   * @param keyExtractor The function applied to a tuple of this streamlet to get the key
   * @param identity The identity element is the initial value for each key
   * @param reduceFn The reduce function that you want to apply to all the values of a key.
   */
  @Override
  public <K, T> KVStreamlet<K, T> reduceByKey(SerializableFunction<R, K> keyExtractor,
                                              T identity,
                                              SerializableBiFunction<T, R, ? extends T> reduceFn) {
    checkNotNull(keyExtractor, "keyExtractor cannot be null");
    checkNotNull(identity, "identity cannot be null");
    checkNotNull(reduceFn, "reduceFn cannot be null");

    GeneralReduceByKeyStreamlet<R, K, T> retval =
        new GeneralReduceByKeyStreamlet<>(this, keyExtractor, identity, reduceFn);
    addChild(retval);
    return new KVStreamletShadow<K, T>(retval);
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
  public <K, T> KVStreamlet<KeyedWindow<K>, T> reduceByKeyAndWindow(
      SerializableFunction<R, K> keyExtractor, SerializableFunction<R, T> valueExtractor,
      WindowConfig windowCfg, SerializableBinaryOperator<T> reduceFn) {
    checkNotNull(keyExtractor, "keyExtractor cannot be null");
    checkNotNull(valueExtractor, "valueExtractor cannot be null");
    checkNotNull(windowCfg, "windowCfg cannot be null");
    checkNotNull(reduceFn, "reduceFn cannot be null");

    ReduceByKeyAndWindowStreamlet<R, K, T> retval =
        new ReduceByKeyAndWindowStreamlet<>(this, keyExtractor, valueExtractor,
            windowCfg, reduceFn);
    addChild(retval);
    return new KVStreamletShadow<KeyedWindow<K>, T>(retval);
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
  public <K, T> KVStreamlet<KeyedWindow<K>, T> reduceByKeyAndWindow(
      SerializableFunction<R, K> keyExtractor, WindowConfig windowCfg,
      T identity, SerializableBiFunction<T, R, ? extends T> reduceFn) {
    checkNotNull(keyExtractor, "keyExtractor cannot be null");
    checkNotNull(windowCfg, "windowCfg cannot be null");
    checkNotNull(identity, "identity cannot be null");
    checkNotNull(reduceFn, "reduceFn cannot be null");

    GeneralReduceByKeyAndWindowStreamlet<R, K, T> retval =
        new GeneralReduceByKeyAndWindowStreamlet<>(this, keyExtractor, windowCfg,
            identity, reduceFn);
    addChild(retval);
    return new KVStreamletShadow<KeyedWindow<K>, T>(retval);
  }

  /**
   * Returns a new Streamlet that is the union of this and the ‘other’ streamlet. Essentially
   * the new streamlet will contain tuples belonging to both Streamlets
  */
  @Override
  public Streamlet<R> union(Streamlet<? extends R> otherStreamlet) {
    checkNotNull(otherStreamlet, "otherStreamlet cannot be null");

    StreamletImpl<? extends R> joinee = (StreamletImpl<? extends R>) otherStreamlet;
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
  public StreamletBase<R> log() {
    LogStreamlet<R> logger = new LogStreamlet<>(this);
    addChild(logger);
    return logger;
  }

  /**
   * Applies the consumer function for every element of this streamlet
   * @param consumer The user supplied consumer function that is invoked for each element
   */
  @Override
  public StreamletBase<R> consume(SerializableConsumer<R> consumer) {
    checkNotNull(consumer, "consumer cannot be null");

    ConsumerStreamlet<R> consumerStreamlet = new ConsumerStreamlet<>(this, consumer);
    addChild(consumerStreamlet);
    return consumerStreamlet;
  }

  /**
   * Uses the sink to consume every element of this streamlet
   * @param sink The Sink that consumes
   */
  @Override
  public StreamletBase<R> toSink(Sink<R> sink) {
    checkNotNull(sink, "sink cannot be null");

    SinkStreamlet<R> sinkStreamlet = new SinkStreamlet<>(this, sink);
    addChild(sinkStreamlet);
    return sinkStreamlet;
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
    checkNotNull(serializableTransformer, "serializableTransformer cannot be null");

    TransformStreamlet<R, T> transformStreamlet =
        new TransformStreamlet<>(this, serializableTransformer);
    addChild(transformStreamlet);
    return transformStreamlet;
  }

  /**
   * Returns a new Streamlet by applying the operator on each element of this streamlet.
   * @param operator The operator to be applied
   * @param <T> The return type of the transform
   * @return Streamlet containing the output of the operation
   */
  @Override
  public <T> Streamlet<T> applyOperator(IStreamletOperator<R, T> operator) {
    checkNotNull(operator, "operator cannot be null");

    // By default, NoneStreamGrouping stategy is used. In this stategy, tuples are forwarded
    // from parent component to a ramdon one of all the instances of the child component,
    // which is the same logic as shuffle grouping.
    return applyOperator(operator, new NoneStreamGrouping());
  }

  /**
   * Returns a new Streamlet by applying the operator on each element of this streamlet.
   * @param operator The operator to be applied
   * @param grouper The grouper to be applied with the operator
   * @param <T> The return type of the transform
   * @return Streamlet containing the output of the operation
   */
  @Override
  public <T> Streamlet<T> applyOperator(IStreamletOperator<R, T> operator, StreamGrouping grouper) {
    checkNotNull(operator, "operator can't be null");
    checkNotNull(grouper, "grouper can't be null");

    StreamletImpl<T> customStreamlet = new CustomStreamlet<>(this, operator, grouper);
    addChild(customStreamlet);
    return customStreamlet;
  }

  /**
   * Returns multiple streams by splitting incoming stream.
   * @param splitFns The Split Functions that test if the tuple should be emitted into each stream
   * Note that there could be 0 or multiple target stream ids
   */
  @Override
  public Streamlet<R> split(Map<String, SerializablePredicate<R>> splitFns) {
    // Make sure map and stream ids are not empty
    require(splitFns.size() > 0, "At least one entry is required");
    require(splitFns.keySet().stream().allMatch(stream -> StringUtils.isNotBlank(stream)),
            "Stream Id can not be blank");

    SplitStreamlet<R> splitStreamlet = new SplitStreamlet<R>(this, splitFns);
    addChild(splitStreamlet);
    return splitStreamlet;
  }

  /**
   * Return a new KVStreamlet<K, R> by applying key extractor to each element of this Streamlet
   * @param keyExtractor The function applied to a tuple of this streamlet to get the key
   */
  @Override
  public <K> KVStreamlet<K, R> keyBy(SerializableFunction<R, K> keyExtractor) {
    return keyBy(keyExtractor, (a) -> a);
  }

  /**
   * Return a new KVStreamlet<K, V> by applying key and value extractor to each element of this
   * Streamlet
   * @param keyExtractor The function applied to a tuple of this streamlet to get the key
   * @param valueExtractor The function applied to a tuple of this streamlet to extract the value
   */
  public <K, V> KVStreamlet<K, V> keyBy(SerializableFunction<R, K> keyExtractor,
                                        SerializableFunction<R, V> valueExtractor) {
    checkNotNull(keyExtractor, "keyExtractor cannot be null");
    checkNotNull(valueExtractor, "valueExtractor cannot be null");

    KeyByStreamlet<R, K, V> retval =
        new KeyByStreamlet<R, K, V>(this, keyExtractor, valueExtractor);
    addChild(retval);
    return new KVStreamletShadow<K, V>(retval);
  }

  /**
   * Returns a new stream of <key, count> by counting tuples in this stream on each key.
   * @param keyExtractor The function applied to a tuple of this streamlet to get the key
   */
  @Override
  public <K> KVStreamlet<K, Long>
      countByKey(SerializableFunction<R, K> keyExtractor) {
    checkNotNull(keyExtractor, "keyExtractor cannot be null");

    CountByKeyStreamlet<R, K> retval = new CountByKeyStreamlet<>(this, keyExtractor);
    addChild(retval);
    return new KVStreamletShadow<K, Long>(retval);
  }


  /**
   * Returns a new stream of <key, count> by counting tuples over a window in this stream on each key.
   * @param keyExtractor The function applied to a tuple of this streamlet to get the key
   * @param windowCfg This is a specification of what kind of windowing strategy you like to have.
   * Typical windowing strategies are sliding windows and tumbling windows
   * Note that there could be 0 or multiple target stream ids
   */
  @Override
  public <K> KVStreamlet<KeyedWindow<K>, Long> countByKeyAndWindow(
      SerializableFunction<R, K> keyExtractor, WindowConfig windowCfg) {
    checkNotNull(keyExtractor, "keyExtractor cannot be null");
    checkNotNull(windowCfg, "windowCfg cannot be null");

    CountByKeyAndWindowStreamlet<R, K> retval =
        new CountByKeyAndWindowStreamlet<>(this, keyExtractor, windowCfg);
    addChild(retval);
    return new KVStreamletShadow<KeyedWindow<K>, Long>(retval);
  }
}
