package org.apache.storm.spout;

public interface IMultiSchemableSpout {
  MultiScheme getScheme();
  void setScheme(MultiScheme scheme);
}
