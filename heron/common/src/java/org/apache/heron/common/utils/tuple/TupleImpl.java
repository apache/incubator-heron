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

package org.apache.heron.common.utils.tuple;

import java.util.List;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.api.topology.TopologyContext;
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;
import org.apache.heron.proto.system.HeronTuples;

/**
 * The tuple is the main data structure in Heron. A tuple is a named list of values,
 * where each value can be any type. Tuples are dynamically typed -- the types of the fields
 * do not need to be declared. Tuples have helper methods like getInteger and getString
 * to get field values without having to cast the result.
 * <p>
 * Heron needs to know how to serialize all the values in a tuple. By default, Heron
 * knows how to serialize the primitive types, strings, and byte arrays. If you want to
 * use another type, you'll need to implement and register a serializer for that type.
 *
 * @see <a href="https://storm.apache.org/documentation/Serialization.html">Storm serialization</a>
 */
public class TupleImpl implements Tuple {
  private static final long serialVersionUID = -5524957157094337394L;
  private final Fields fields;
  private final TopologyAPI.StreamId stream;
  private final long tupleKey;
  private final List<HeronTuples.RootId> roots;
  private final long creationTime;
  private final int sourceTaskId;

  private List<Object> values;

  public TupleImpl(TopologyContext context, TopologyAPI.StreamId stream,
                   long tupleKey, List<HeronTuples.RootId> roots,
                   List<Object> values, int sourceTaskId) {
    this(context, stream, tupleKey, roots, values, System.nanoTime(), true, sourceTaskId);
  }

  public TupleImpl(TopologyContext context, TopologyAPI.StreamId stream,
                   long tupleKey, List<HeronTuples.RootId> roots,
                   List<Object> values, long creationTime, boolean isCheckRequired,
                   int sourceTaskId) {
    this.stream = stream;
    this.tupleKey = tupleKey;
    this.roots = roots;
    this.values = values;
    this.creationTime = creationTime;
    this.sourceTaskId = sourceTaskId;
    this.fields = context.getComponentOutputFields(
        this.stream.getComponentName(), this.stream.getId());

    if (isCheckRequired) {
      if (values.size() != this.fields.size()) {
        throw new IllegalArgumentException(
            "Tuple created with wrong number of fields. "
                + "Expected " + this.fields.size() + " fields but got "
                + values.size() + " fields"
        );
      }
    }
  }

  public List<HeronTuples.RootId> getRoots() {
    return roots;
  }

  public long getTupleKey() {
    return tupleKey;
  }

  @Override
  public int size() {
    return values.size();
  }

  @Override
  public int fieldIndex(String field) {
    return getFields().fieldIndex(field);
  }

  @Override
  public boolean contains(String field) {
    return getFields().contains(field);
  }

  @Override
  public Object getValue(int i) {
    return values.get(i);
  }

  @Override
  public String getString(int i) {
    return (String) values.get(i);
  }

  @Override
  public Integer getInteger(int i) {
    return (Integer) values.get(i);
  }

  @Override
  public Long getLong(int i) {
    return (Long) values.get(i);
  }

  @Override
  public Boolean getBoolean(int i) {
    return (Boolean) values.get(i);
  }

  @Override
  public Short getShort(int i) {
    return (Short) values.get(i);
  }

  @Override
  public Byte getByte(int i) {
    return (Byte) values.get(i);
  }

  @Override
  public Double getDouble(int i) {
    return (Double) values.get(i);
  }

  @Override
  public Float getFloat(int i) {
    return (Float) values.get(i);
  }

  @Override
  public byte[] getBinary(int i) {
    return (byte[]) values.get(i);
  }

  @Override
  public Object getValueByField(String field) {
    return values.get(fieldIndex(field));
  }

  @Override
  public String getStringByField(String field) {
    return (String) values.get(fieldIndex(field));
  }

  @Override
  public Integer getIntegerByField(String field) {
    return (Integer) values.get(fieldIndex(field));
  }

  @Override
  public Long getLongByField(String field) {
    return (Long) values.get(fieldIndex(field));
  }

  @Override
  public Boolean getBooleanByField(String field) {
    return (Boolean) values.get(fieldIndex(field));
  }

  @Override
  public Short getShortByField(String field) {
    return (Short) values.get(fieldIndex(field));
  }

  @Override
  public Byte getByteByField(String field) {
    return (Byte) values.get(fieldIndex(field));
  }

  @Override
  public Double getDoubleByField(String field) {
    return (Double) values.get(fieldIndex(field));
  }

  @Override
  public Float getFloatByField(String field) {
    return (Float) values.get(fieldIndex(field));
  }

  @Override
  public byte[] getBinaryByField(String field) {
    return (byte[]) values.get(fieldIndex(field));
  }

  @Override
  public List<Object> getValues() {
    return values;
  }

  @Override
  public Fields getFields() {
    return this.fields;
  }

  @Override
  public List<Object> select(Fields selector) {
    return getFields().select(selector, values);
  }

  @Override
  public TopologyAPI.StreamId getSourceGlobalStreamId() {
    return stream;
  }

  @Override
  public String getSourceComponent() {
    return stream.getComponentName();
  }

  @Override
  public int getSourceTask() {
    return sourceTaskId;
  }

  @Override
  public String getSourceStreamId() {
    return stream.getId();
  }

  @Override
  public String toString() {
    return "source: " + getSourceComponent() + ", stream: " + getSourceStreamId()
        + ", " + values.toString();
  }

  @Override
  public boolean equals(Object other) {
    return this == other;
  }

  @Override
  public int hashCode() {
    return System.identityHashCode(this);
  }

  @Override
  public void resetValues() {
    values = null;
  }

  public long getCreationTime() {
    return creationTime;
  }
}

