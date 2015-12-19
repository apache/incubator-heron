package com.twitter.heron.api.spout;

import java.util.List;
import java.io.Serializable;

import com.twitter.heron.api.tuple.Fields;

public interface MultiScheme extends Serializable {
  public Iterable<List<Object>> deserialize(byte[] ser);
  public Fields getOutputFields();
}
