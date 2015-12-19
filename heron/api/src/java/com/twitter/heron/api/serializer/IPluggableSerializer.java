package com.twitter.heron.api.serializer;

import java.util.Map;

public interface IPluggableSerializer {
  public void initialize(Map config);
  public byte[] serialize(Object _object);
  public Object deserialize(byte[] _input);
}
