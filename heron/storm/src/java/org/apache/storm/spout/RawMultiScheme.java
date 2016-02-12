package org.apache.storm.spout;

import java.util.List;

import org.apache.storm.tuple.Fields;

import static java.util.Arrays.asList;
import static org.apache.storm.utils.Utils.tuple;

public class RawMultiScheme implements MultiScheme {
  @Override
  public Iterable<List<Object>> deserialize(byte[] ser) {
    return asList(tuple(ser));
  }

  @Override
  public Fields getOutputFields() {
    return new Fields("bytes");
  }
}
