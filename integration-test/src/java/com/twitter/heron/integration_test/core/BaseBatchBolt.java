package com.twitter.heron.integration_test.core;

import com.twitter.heron.api.bolt.BaseRichBolt;

// We keep this since we want to be consistent with earlier framework to reuse test topologies
public abstract class BaseBatchBolt extends BaseRichBolt implements IBatchBolt {
  private static final long serialVersionUID = 7380672976877532671L;
}
