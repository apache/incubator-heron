package org.apache.storm.spout;

import java.io.Serializable;
import java.util.List;

import org.apache.storm.tuple.Fields;

public interface MultiScheme extends Serializable {
  public Iterable<List<Object>> deserialize(byte[] ser);
  public Fields getOutputFields();
}
