package org.apache.storm.tuple;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.io.Serializable;

public class Fields implements Iterable<String>, Serializable {
  private com.twitter.heron.api.tuple.Fields delegate;

  public Fields(String... fields) {
    delegate = new com.twitter.heron.api.tuple.Fields(fields);
  }

  public Fields(com.twitter.heron.api.tuple.Fields delegate) {
    this.delegate = delegate;
  }
    
  public Fields(List<String> fields) {
    delegate = new com.twitter.heron.api.tuple.Fields(fields);
  }
    
  public List<Object> select(Fields selector, List<Object> tuple) {
    return delegate.select(selector.getDelegate(), tuple);
  }

  public List<String> toList() {
    return delegate.toList();
  }
    
  public int size() {
    return delegate.size();
  }

  public String get(int index) {
    return delegate.get(index);
  }

  public Iterator<String> iterator() {
    return delegate.iterator();
  }
    
  /**
   * Returns the position of the specified field.
   */
  public int fieldIndex(String field) {
    return delegate.fieldIndex(field);
  }
    
  /**
   * Returns true if this contains the specified name of the field.
   */
  public boolean contains(String field) {
    return delegate.contains(field);
  }
    
  public String toString() {
    return delegate.toString();
  }

  public com.twitter.heron.api.tuple.Fields getDelegate() {
    return delegate;
  }
}
