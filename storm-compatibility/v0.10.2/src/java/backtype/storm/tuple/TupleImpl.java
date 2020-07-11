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

package backtype.storm.tuple;

import java.util.List;

/**
 * The tuple is the main data structure in Storm. A tuple is a named list of values,
 * where each value can be any type. Tuples are dynamically typed -- the types of the fields
 * do not need to be declared. Tuples have helper methods like getInteger and getString
 * to get field values without having to cast the result.
 * <p>
 * Storm needs to know how to serialize all the values in a tuple. By default, Storm
 * knows how to serialize the primitive types, strings, and byte arrays. If you want to
 * use another type, you'll need to implement and register a serializer for that type.
 * @see <a href="https://storm.apache.org/documentation/Serialization.html">Storm serialization</a>
 */
public class TupleImpl implements Tuple {
  private org.apache.heron.api.tuple.Tuple delegate;

  public TupleImpl(org.apache.heron.api.tuple.Tuple t) {
    delegate = t;
  }

  public org.apache.heron.api.tuple.Tuple getDelegate() {
    return delegate;
  }

  /**
   * Returns the number of fields in this tuple.
   */
  public int size() {
    return delegate.size();
  }

  /**
   * Returns the position of the specified field in this tuple.
   */
  public int fieldIndex(String field) {
    return delegate.fieldIndex(field);
  }

  /**
   * Returns true if this tuple contains the specified name of the field.
   */
  public boolean contains(String field) {
    return delegate.contains(field);
  }

  /**
   * Gets the field at position i in the tuple. Returns object since tuples are dynamically typed.
   */
  public Object getValue(int i) {
    return delegate.getValue(i);
  }

  /**
   * Returns the String at position i in the tuple. If that field is not a String,
   * you will get a runtime error.
   */
  public String getString(int i) {
    return delegate.getString(i);
  }

  /**
   * Returns the Integer at position i in the tuple. If that field is not an Integer,
   * you will get a runtime error.
   */
  public Integer getInteger(int i) {
    return delegate.getInteger(i);
  }

  /**
   * Returns the Long at position i in the tuple. If that field is not a Long,
   * you will get a runtime error.
   */
  public Long getLong(int i) {
    return delegate.getLong(i);
  }

  /**
   * Returns the Boolean at position i in the tuple. If that field is not a Boolean,
   * you will get a runtime error.
   */
  public Boolean getBoolean(int i) {
    return delegate.getBoolean(i);
  }

  /**
   * Returns the Short at position i in the tuple. If that field is not a Short,
   * you will get a runtime error.
   */
  public Short getShort(int i) {
    return delegate.getShort(i);
  }

  /**
   * Returns the Byte at position i in the tuple. If that field is not a Byte,
   * you will get a runtime error.
   */
  public Byte getByte(int i) {
    return delegate.getByte(i);
  }

  /**
   * Returns the Double at position i in the tuple. If that field is not a Double,
   * you will get a runtime error.
   */
  public Double getDouble(int i) {
    return delegate.getDouble(i);
  }

  /**
   * Returns the Float at position i in the tuple. If that field is not a Float,
   * you will get a runtime error.
   */
  public Float getFloat(int i) {
    return delegate.getFloat(i);
  }

  /**
   * Returns the byte array at position i in the tuple. If that field is not a byte array,
   * you will get a runtime error.
   */
  public byte[] getBinary(int i) {
    return delegate.getBinary(i);
  }


  public Object getValueByField(String field) {
    return delegate.getValueByField(field);
  }

  public String getStringByField(String field) {
    return delegate.getStringByField(field);
  }

  public Integer getIntegerByField(String field) {
    return delegate.getIntegerByField(field);
  }

  public Long getLongByField(String field) {
    return delegate.getLongByField(field);
  }

  public Boolean getBooleanByField(String field) {
    return delegate.getBooleanByField(field);
  }

  public Short getShortByField(String field) {
    return delegate.getShortByField(field);
  }

  public Byte getByteByField(String field) {
    return delegate.getByteByField(field);
  }

  public Double getDoubleByField(String field) {
    return delegate.getDoubleByField(field);
  }

  public Float getFloatByField(String field) {
    return delegate.getFloatByField(field);
  }

  public byte[] getBinaryByField(String field) {
    return delegate.getBinaryByField(field);
  }

  /**
   * Gets all the values in this tuple.
   */
  public List<Object> getValues() {
    return delegate.getValues();
  }

  /**
   * Gets the names of the fields in this tuple.
   */
  public Fields getFields() {
    return new Fields(delegate.getFields());
  }

  /**
   * Returns a subset of the tuple based on the fields selector.
   */
  public List<Object> select(Fields selector) {
    return delegate.select(selector.getDelegate());
  }

  /**
   * Returns the global stream id (component + stream) of this tuple.
   */
  /*
    TODO:- One can get this using getSourceStreamId and getSourceComponent
    public GlobalStreamId getSourceGlobalStreamid();
   */

  /**
   * Gets the id of the component that created this tuple.
   */
  public String getSourceComponent() {
    return delegate.getSourceComponent();
  }

  /**
   * Gets the id of the task that created this tuple.
   */
  public int getSourceTask() {
    return delegate.getSourceTask();
  }

  /**
   * Gets the id of the stream that this tuple was emitted to.
   */
  public String getSourceStreamId() {
    return delegate.getSourceStreamId();
  }

  /**
   * Gets the message id that associated with this tuple.
   */
  /*
    TODO:- does anyone use this
    public MessageId getMessageId();
   */

  /**
   * Resets the tuple values to null
   * TODO:- Is this needed
   */
  public void resetValues() {
    delegate.resetValues();
  }
}
