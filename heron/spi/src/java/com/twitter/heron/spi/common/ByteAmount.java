// Copyright 2016 Twitter. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package com.twitter.heron.spi.common;

/**
 * Class that encapsulates number of bytes, with helpers to handle units properly.
 */
public class ByteAmount implements Comparable<ByteAmount> {
  private static final long GB = 1024L * 1024 * 1024;
  private static final long MB = 1024L * 1024;

  public static final ByteAmount ZERO = ByteAmount.fromBytes(0);
  private final long bytes;

  private ByteAmount(long bytes) {
    this.bytes = bytes;
  }

  public static ByteAmount fromBytes(long bytes) {
    return new ByteAmount(bytes);
  }

  public static ByteAmount fromMegabytes(long megabytes) {
    return new ByteAmount(megabytes * MB);
  }

  public static ByteAmount fromGigabytes(long gigabytes) {
    return new ByteAmount(gigabytes * GB);
  }

  public long asBytes() {
    return bytes;
  }

  public boolean isZero() {
    return ZERO.equals(this);
  }

  public ByteAmount minus(ByteAmount other) {
    return ByteAmount.fromBytes(this.asBytes() - other.asBytes());
  }

  public ByteAmount plus(ByteAmount other) {
    return ByteAmount.fromBytes(this.asBytes() + other.asBytes());
  }

  public ByteAmount multiply(int factor) {
    return ByteAmount.fromBytes(this.asBytes() * factor);
  }

  public ByteAmount divide(int factor) {
    return ByteAmount.fromBytes(this.asBytes() / factor);
  }

  public ByteAmount increaseBy(double percentage) {
    return ByteAmount.fromBytes(Math.round(asBytes() * (1 + (percentage / 100))));
  }

  public boolean greaterThan(ByteAmount other) {
    return this.asBytes() > other.asBytes();
  }

  public boolean greaterOrEqual(ByteAmount other) {
    return this.asBytes() >= other.asBytes();
  }

  public boolean lessThan(ByteAmount other) {
    return this.asBytes() < other.asBytes();
  }

  public boolean lessOrEqual(ByteAmount other) {
    return this.asBytes() <= other.asBytes();
  }

  public ByteAmount max(ByteAmount other) {
    if (this.greaterThan(other)) {
      return this;
    } else {
      return other;
    }
  }

  @Override
  public int compareTo(ByteAmount other) {
    return Long.compare(asBytes(), other.asBytes());
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) return true;
    if (other == null || getClass() != other.getClass()) {
      return false;
    }

    ByteAmount that = (ByteAmount) other;
    return bytes == that.bytes;
  }

  @Override
  public int hashCode() {
    return (int) (bytes ^ (bytes >>> 32));
  }

  @Override
  public String toString() {
    String value;
    if (bytes > GB) {
      value = "gigabytes=" + bytes / GB;
    } else if (bytes > MB) {
      value = "megabytes=" + bytes / MB;
    } else {
      value = "bytes=" + bytes;
    }
    return String.format("ByteAmount{%s}", value);
  }
}
