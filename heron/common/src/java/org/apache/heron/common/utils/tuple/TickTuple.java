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
import org.apache.heron.api.tuple.Fields;
import org.apache.heron.api.tuple.Tuple;

/**
 * This is the tick tuple as seen by the bolt.
 * where each value can be any type. Tuples are dynamically typed -- the types of the fields
 * do not need to be declared. Tuples have helper methods like getInteger and getString
 * to get field values without having to cast the result.
 * <p>
 * Heron needs to know how to serialize all the values in a tuple. By default, Heron
 * knows how to serialize the primitive types, strings, and byte arrays. If you want to
 * use another type, you'll need to implement and register a serializer for that type.
 * @see <a href="https://storm.apache.org/documentation/Serialization.html">Storm serialization</a>
 */
public class TickTuple implements Tuple {
  private static final long serialVersionUID = -7405457325549296084L;

  public TickTuple() {
  }

  @Override
  public int size() {
    return 0;
  }

  @Override
  public int fieldIndex(String field) {
    return -1;
  }

  @Override
  public boolean contains(String field) {
    return false;
  }

  @Override
  public Object getValue(int i) {
    throw new RuntimeException("Cannot call getValue for TickTuple");
  }

  @Override
  public String getString(int i) {
    throw new RuntimeException("Cannot call getString for TickTuple");
  }

  @Override
  public Integer getInteger(int i) {
    throw new RuntimeException("Cannot call getInteger for TickTuple");
  }

  @Override
  public Long getLong(int i) {
    throw new RuntimeException("Cannot call getLong for TickTuple");
  }

  @Override
  public Boolean getBoolean(int i) {
    throw new RuntimeException("Cannot call getBoolean for TickTuple");
  }

  @Override
  public Short getShort(int i) {
    throw new RuntimeException("Cannot call getShort for TickTuple");
  }

  @Override
  public Byte getByte(int i) {
    throw new RuntimeException("Cannot call getByte for TickTuple");
  }

  @Override
  public Double getDouble(int i) {
    throw new RuntimeException("Cannot call getDouble for TickTuple");
  }

  @Override
  public Float getFloat(int i) {
    throw new RuntimeException("Cannot call getFloat for TickTuple");
  }

  @Override
  public byte[] getBinary(int i) {
    throw new RuntimeException("Cannot call getBinary for TickTuple");
  }

  @Override
  public Object getValueByField(String field) {
    throw new RuntimeException("Cannot call getValueByField for TickTuple");
  }

  @Override
  public String getStringByField(String field) {
    throw new RuntimeException("Cannot call getStringByField for TickTuple");
  }

  @Override
  public Integer getIntegerByField(String field) {
    throw new RuntimeException("Cannot call getIntegerByField for TickTuple");
  }

  @Override
  public Long getLongByField(String field) {
    throw new RuntimeException("Cannot call getLongByField for TickTuple");
  }

  @Override
  public Boolean getBooleanByField(String field) {
    throw new RuntimeException("Cannot call getBooleanByField for TickTuple");
  }

  @Override
  public Short getShortByField(String field) {
    throw new RuntimeException("Cannot call getShortByField for TickTuple");
  }

  @Override
  public Byte getByteByField(String field) {
    throw new RuntimeException("Cannot call getByteByField for TickTuple");
  }

  @Override
  public Double getDoubleByField(String field) {
    throw new RuntimeException("Cannot call getDoubleByField for TickTuple");
  }

  @Override
  public Float getFloatByField(String field) {
    throw new RuntimeException("Cannot call getFloatByField for TickTuple");
  }

  @Override
  public byte[] getBinaryByField(String field) {
    throw new RuntimeException("Cannot call getBinaryByField for TickTuple");
  }

  @Override
  public List<Object> getValues() {
    return null;
  }

  @Override
  public Fields getFields() {
    return null;
  }

  @Override
  public List<Object> select(Fields selector) {
    return null;
  }

  @Override
  public TopologyAPI.StreamId getSourceGlobalStreamId() {
    throw new RuntimeException("Cannot call getSourceGlobalStreamId for TickTuple");
  }

  @Override
  public String getSourceComponent() {
    return "__system";
  }

  // TODO:- Is this needed
  @Override
  public int getSourceTask() {
    throw new RuntimeException("Tuple no longer supports getSourceTask");
  }

  @Override
  public String getSourceStreamId() {
    return "__tick";
  }

  @Override
  public String toString() {
    return "source: " + getSourceComponent() + ", stream: " + getSourceStreamId();
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
  }
}

