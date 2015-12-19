package com.twitter.heron.api.spout;

public interface IMultiSchemableSpout {
  MultiScheme getScheme();
  void setScheme(MultiScheme scheme);
}
