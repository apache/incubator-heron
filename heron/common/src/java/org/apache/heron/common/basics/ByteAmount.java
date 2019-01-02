/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.heron.common.basics;

/**
 * Class that encapsulates number of bytes, with helpers to handle units properly.
 */
public final class ByteAmount implements ResourceMeasure<ByteAmount> {
  private static final long KB = 1024L;
  private static final long MB = KB * 1024;
  private static final long GB = MB * 1024;
  @SuppressWarnings("MathRoundIntLong")
  private static final long MAX_MB = Math.round(Long.MAX_VALUE / MB);
  @SuppressWarnings("MathRoundIntLong")
  private static final long MAX_GB = Math.round(Long.MAX_VALUE / GB);
  @SuppressWarnings("MathRoundIntLong")
  private static final long MAX_KB = Math.round(Long.MAX_VALUE / KB);

  public static final ByteAmount ZERO = ByteAmount.fromBytes(0);
  private final long bytes;

  private ByteAmount(long bytes) {
    this.bytes = bytes;
  }

  /**
   * Creates a ByteAmount value in bytes.
   *
   * @param bytes value in bytes to represent
   * @return a ByteAmount object repressing the number of bytes passed
   */
  public static ByteAmount fromBytes(long bytes) {
    return new ByteAmount(bytes);
  }

  /**
   * Creates a ByteAmount value in megabytes. If the megabytes value
   * is $lt;= Long.MAX_VALUE / 1024 / 1024, the byte representation is capped at Long.MAX_VALUE.
   *
   * @param megabytes value in megabytes to represent
   * @return a ByteAmount object repressing the number of MBs passed
   */
  public static ByteAmount fromMegabytes(long megabytes) {
    if (megabytes >= MAX_MB) {
      return new ByteAmount(Long.MAX_VALUE);
    } else {
      return new ByteAmount(megabytes * MB);
    }
  }

  /**
   * Creates a ByteAmount value in gigabytes. If the gigabytes value
   * is &gt;= Long.MAX_VALUE / 1024 / 1024 / 1024, the byte representation is capped at Long.MAX_VALUE.
   *
   * @param gigabytes value in gigabytes to represent
   * @return a ByteAmount object repressing the number of GBs passed
   */
  public static ByteAmount fromGigabytes(long gigabytes) {
    if (gigabytes >= MAX_GB) {
      return new ByteAmount(Long.MAX_VALUE);
    } else {
      return new ByteAmount(gigabytes * GB);
    }
  }

  /**
   * Returns the number of bytes associated with the ByteValue.
   * @return number of bytes
   */
  public long asBytes() {
    return bytes;
  }

  /**
   * Converts the number of bytes to megabytes, rounding if there is a remainder. Because of loss
   * of precision due to rounding, it's strongly advised to only use this method when it is certain
   * that the only operations performed on the object were multiplication or addition and
   * subtraction of other megabytes. If division or increaseBy were used this method could round up
   * or down, potentially yielding unexpected results.
   * @return returns the ByteValue in MBs or 0 if the value is &lt; (1024 * 1024) / 2
   */
  public long asMegabytes() {
    return Math.round((double) bytes / MB);
  }

  /**
   * Converts the number of bytes to kilobytes, rounding if there is a remainder. Because of loss
   * of precision due to rounding, it's strongly advised to only use this method when it is certain
   * that the only operations performed on the object were multiplication or addition and
   * subtraction of other megabytes. If division or increaseBy were used this method could round up
   * or down, potentially yielding unexpected results.
   * @return returns the ByteValue in KBs or 0 if the value is &lt; (1024) / 2
   */
  public long asKilobytes() {
    return Math.round((double) bytes / KB);
  }

  /**
   * Converts the number of bytes to gigabytes, rounding if there is a remainder. Because of loss
   * of precision due to rounding, it's strongly advised to only use this method when it is certain
   * that the only operations performed on the object were multiplication or addition and
   * subtraction of other gigabytes. If division or increaseBy were used this method could round up
   * or down, potentially yielding unexpected results.
   * @return returns the ByteValue in GBs or 0 if the value is &lt; (1024 * 1024 * 1024) / 2
   */
  public long asGigabytes() {
    return Math.round((double) bytes / GB);
  }

  /**
   * Convenience methdod to determine if byte value is zero
   * @return true if the byte value is 0
   */
  public boolean isZero() {
    return ZERO.equals(this);
  }

  /**
   * Subtracts other from this.
   * @param other ByteValue to subtract
   * @return a new ByteValue of this minus other ByteValue
   * @throws IllegalArgumentException if subtraction would overshoot Long.MIN_VALUE
   */
  public ByteAmount minus(ByteAmount other) {
    checkArgument(Long.MIN_VALUE + other.asBytes() <= asBytes(), String.format(
        "Subtracting %s from %s would overshoot Long.MIN_LONG", other, this));
    return ByteAmount.fromBytes(asBytes() - other.asBytes());
  }

  /**
   * Adds other to this.
   * @param other ByteValue to add
   * @return a new ByteValue of this plus other ByteValue
   * @throws IllegalArgumentException if addition would exceed Long.MAX_VALUE
   */
  public ByteAmount plus(ByteAmount other) {
    checkArgument(Long.MAX_VALUE - asBytes() >= other.asBytes(), String.format(
        "Adding %s to %s would exceed Long.MAX_LONG", other, this));
    return ByteAmount.fromBytes(asBytes() + other.asBytes());
  }

  /**
   * Multiplies by factor
   * @param factor value to multiply by
   * @return a new ByteValue of this ByteValue multiplied by factor
   * @throws IllegalArgumentException if multiplication would exceed Long.MAX_VALUE
   */
  public ByteAmount multiply(int factor) {
    checkArgument(asBytes() <= Long.MAX_VALUE / factor, String.format(
        "Multiplying %s by %d would exceed Long.MAX_LONG", this, factor));
    return ByteAmount.fromBytes(asBytes() * factor);
  }

  /**
   * Divides by factor, rounding any remainder. For example 10 bytes / 6 = 1.66 becomes 2. Use
   * caution when dividing and be aware that because of precision lose due to round-off, dividing by
   * X and multiplying back by X might not return the initial value.
   * @param factor value to divide by
   * @return a new ByteValue of this ByteValue divided by factor
   */
  public ByteAmount divide(int factor) {
    checkArgument(factor != 0, String.format("Can not divide %s by 0", this));
    return ByteAmount.fromBytes(Math.round((double) this.asBytes() / (double) factor));
  }

  /**
   * Increases by a percentage, rounding any remainder. Be aware that because of rounding, increases
   * will be approximate to the nearest byte.
   * @param percentage value to increase by
   * @return a new ByteValue of this ByteValue increased by percentage
   * @throws IllegalArgumentException if increase would exceed Long.MAX_VALUE
   */
  public ByteAmount increaseBy(int percentage) {
    checkArgument(percentage >= 0, String.format(
        "Increasing by negative percent (%d) not supported", percentage));
    double factor = 1.0 + ((double) percentage / 100);
    long max = Math.round(Long.MAX_VALUE / factor);
    checkArgument(asBytes() <= max, String.format(
        "Increasing %s by %d percent would exceed Long.MAX_LONG", this, percentage));
    return ByteAmount.fromBytes(Math.round((double) asBytes() * factor));
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
    if (this == other) {
      return true;
    }
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
    if (asGigabytes() > 0) {
      value = String.format("%.1f GB (%d bytes)", (double) asBytes() / GB, asBytes());
    } else if (asMegabytes() > 0) {
      value = String.format("%.1f MB (%d bytes)", (double) asBytes() / MB, asBytes());
    } else if (asKilobytes() > 0) {
      value = String.format("%.1f KB (%d bytes)", (double) asBytes() / KB, asBytes());
    } else {
      value = bytes + " bytes";
    }
    return String.format("ByteAmount{%s}", value);
  }

  private void checkArgument(boolean condition, String errorMessage) {
    if (!condition) {
      throw new IllegalArgumentException(errorMessage);
    }
  }
}
