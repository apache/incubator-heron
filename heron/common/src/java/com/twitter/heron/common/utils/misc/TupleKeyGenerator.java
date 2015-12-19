package com.twitter.heron.common.utils.misc;

import java.util.Random;

/**
 * Generate tuple key by using Random Class
 */

public class TupleKeyGenerator {
  private final Random rand;

  public TupleKeyGenerator() {
    rand = new Random();
  }

  public long next() {
    return rand.nextLong();
  }
}