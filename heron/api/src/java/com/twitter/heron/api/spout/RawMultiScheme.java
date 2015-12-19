package com.twitter.heron.api.spout;

import java.util.List;

import com.twitter.heron.api.tuple.Fields;


import static com.twitter.heron.api.utils.Utils.tuple;
import static java.util.Arrays.asList;

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
