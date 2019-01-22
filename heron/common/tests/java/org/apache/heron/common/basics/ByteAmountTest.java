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

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class ByteAmountTest {
  private static final long MB = 1024 * 1024;
  private static final long GB = MB * 1024;

  @Test
  public void testConversions() {
    assertEquals(2, ByteAmount.fromBytes(2).asBytes());
    assertEquals(2 * MB, ByteAmount.fromMegabytes(2).asBytes());
    assertEquals(2 * GB, ByteAmount.fromGigabytes(2).asBytes());

    assertEquals(0, ByteAmount.fromBytes(2).asGigabytes());
    assertEquals(0, ByteAmount.fromBytes(2).asMegabytes());

    assertEquals(MB - 1, ByteAmount.fromBytes(MB - 1).asBytes());
    assertEquals(1, ByteAmount.fromBytes(MB - 1).asMegabytes());
    assertEquals(0, ByteAmount.fromBytes(MB - 1).asGigabytes());

    assertEquals(GB - 1, ByteAmount.fromBytes(GB - 1).asBytes());
    assertEquals(1024, ByteAmount.fromBytes(GB - 1).asMegabytes());
    assertEquals(1, ByteAmount.fromBytes(GB - 1).asGigabytes());

    assertEquals(5, ByteAmount.fromMegabytes(5).asMegabytes());
    assertEquals(5, ByteAmount.fromGigabytes(5).asGigabytes());

    assertEquals(1, ByteAmount.fromBytes((int) (MB * 1.5) - 1).asMegabytes());
    assertEquals(2, ByteAmount.fromBytes((int) (MB * 1.5) + 1).asMegabytes());

    assertEquals(1, ByteAmount.fromBytes((int) (GB * 1.5) - 1).asGigabytes());
    assertEquals(2, ByteAmount.fromBytes((int) (GB * 1.5) + 1).asGigabytes());

    assertEquals(Long.MAX_VALUE, ByteAmount.fromBytes(Long.MAX_VALUE).asBytes());
    assertEquals(Long.MAX_VALUE, ByteAmount.fromMegabytes(Long.MAX_VALUE).asBytes());
    assertEquals(Long.MAX_VALUE, ByteAmount.fromGigabytes(Long.MAX_VALUE).asBytes());

    assertEquals(Long.MAX_VALUE - 1, ByteAmount.fromBytes(Long.MAX_VALUE - 1).asBytes());
    assertEquals(Long.MAX_VALUE, ByteAmount.fromMegabytes(Long.MAX_VALUE - 1).asBytes());
    assertEquals(Long.MAX_VALUE, ByteAmount.fromGigabytes(Long.MAX_VALUE - 1).asBytes());

    assertTrue(ByteAmount.ZERO.isZero());
    assertTrue(ByteAmount.fromBytes(0).isZero());
    assertFalse(ByteAmount.fromBytes(1).isZero());
  }

  @Test
  public void testAddition() {
    assertEquals(9, ByteAmount.fromBytes(4).plus(ByteAmount.fromBytes(5)).asBytes());
    assertEquals(MB + 1, ByteAmount.fromBytes(1).plus(ByteAmount.fromMegabytes(1)).asBytes());
    assertEquals(GB + 1, ByteAmount.fromBytes(1).plus(ByteAmount.fromGigabytes(1)).asBytes());
    assertEquals(Long.MAX_VALUE,
        ByteAmount.fromBytes(Long.MAX_VALUE - 1).plus(ByteAmount.fromBytes(1)).asBytes());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAdditionOverflow() {
    ByteAmount.fromBytes(Long.MAX_VALUE).plus(ByteAmount.fromBytes(1));
  }

  @Test
  public void testSubtraction() {
    assertEquals(4, ByteAmount.fromBytes(9).minus(ByteAmount.fromBytes(5)).asBytes());
    assertEquals(MB - 1, ByteAmount.fromMegabytes(1).minus(ByteAmount.fromBytes(1)).asBytes());
    assertEquals(GB - 1, ByteAmount.fromGigabytes(1).minus(ByteAmount.fromBytes(1)).asBytes());
    assertEquals(Long.MIN_VALUE,
        ByteAmount.fromBytes(-1).minus(ByteAmount.fromBytes(Long.MAX_VALUE)).asBytes());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSubtractionOverflow() {
    ByteAmount.fromBytes(-2).minus(ByteAmount.fromBytes(Long.MAX_VALUE));
  }

  @Test
  public void testMultiplication() {
    assertEquals(0, ByteAmount.fromBytes(0).multiply(2).asBytes());
    assertEquals(45, ByteAmount.fromBytes(9).multiply(5).asBytes());
    assertEquals(MB * 2, ByteAmount.fromMegabytes(1).multiply(2).asBytes());
    assertEquals(GB * 2, ByteAmount.fromGigabytes(1).multiply(2).asBytes());
    assertEquals(ByteAmount.fromBytes(Long.MAX_VALUE - 1),
        ByteAmount.fromBytes(Long.MAX_VALUE / 2).multiply(2));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMultiplicationOverflow() {
    ByteAmount.fromBytes((Long.MAX_VALUE / 2) + 1).multiply(2);
  }

  @Test
  public void testDivision() {
    assertEquals(0, ByteAmount.fromBytes(0).divide(2).asBytes());
    assertEquals(1, ByteAmount.fromBytes(1).divide(2).asBytes());
    assertEquals(1, ByteAmount.fromBytes(2).divide(2).asBytes());
    assertEquals(2, ByteAmount.fromBytes(3).divide(2).asBytes());
    assertEquals(2, ByteAmount.fromBytes(10).divide(6).asBytes());
    assertEquals(3, ByteAmount.fromBytes(15).divide(5).asBytes());
    assertEquals(MB / 2, ByteAmount.fromMegabytes(1).divide(2).asBytes());
    assertEquals(GB / 2, ByteAmount.fromGigabytes(1).divide(2).asBytes());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testDivideByZero() {
    ByteAmount.fromBytes(1).divide(0);
  }

  @Test
  public void testIncreaseBy() {
    assertEquals(0, ByteAmount.fromBytes(0).increaseBy(2).asBytes());
    assertEquals(11, ByteAmount.fromBytes(10).increaseBy(10).asBytes());
    assertEquals(11, ByteAmount.fromBytes(10).increaseBy(14).asBytes());
    assertEquals(12, ByteAmount.fromBytes(10).increaseBy(15).asBytes());
    ByteAmount.fromBytes(Long.MAX_VALUE / 2 + 1).increaseBy(100);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIncreaseByOverflow() {
    ByteAmount.fromBytes((Long.MAX_VALUE / 2) + 2).increaseBy(100);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIncreaseByNegative() {
    ByteAmount.fromBytes(1).increaseBy(-1);
  }

  @Test
  public void testComparison() {
    assertTrue(ByteAmount.fromBytes(0).lessThan(ByteAmount.fromBytes(1)));
    assertFalse(ByteAmount.fromBytes(1).lessThan(ByteAmount.fromBytes(1)));
    assertFalse(ByteAmount.fromBytes(2).lessThan(ByteAmount.fromBytes(1)));

    assertTrue(ByteAmount.fromBytes(0).lessOrEqual(ByteAmount.fromBytes(1)));
    assertTrue(ByteAmount.fromBytes(1).lessOrEqual(ByteAmount.fromBytes(1)));
    assertFalse(ByteAmount.fromBytes(2).lessOrEqual(ByteAmount.fromBytes(1)));

    assertFalse(ByteAmount.fromBytes(0).greaterThan(ByteAmount.fromBytes(1)));
    assertFalse(ByteAmount.fromBytes(1).greaterThan(ByteAmount.fromBytes(1)));
    assertTrue(ByteAmount.fromBytes(2).greaterThan(ByteAmount.fromBytes(1)));

    assertFalse(ByteAmount.fromBytes(0).greaterOrEqual(ByteAmount.fromBytes(1)));
    assertTrue(ByteAmount.fromBytes(1).greaterOrEqual(ByteAmount.fromBytes(1)));
    assertTrue(ByteAmount.fromBytes(2).greaterOrEqual(ByteAmount.fromBytes(1)));
  }

  @Test
  public void testMax() {
    assertEquals(ByteAmount.fromBytes(1), ByteAmount.fromBytes(0).max(ByteAmount.fromBytes(1)));
    assertEquals(ByteAmount.fromBytes(1), ByteAmount.fromBytes(1).max(ByteAmount.fromBytes(1)));
    assertEquals(ByteAmount.fromBytes(2), ByteAmount.fromBytes(2).max(ByteAmount.fromBytes(1)));
  }

  @Test
  public void testEquals() {
    assertFalse(ByteAmount.fromBytes(0).equals(ByteAmount.fromBytes(1)));
    assertTrue(ByteAmount.fromBytes(1).equals(ByteAmount.fromBytes(1)));
    assertFalse(ByteAmount.fromBytes(2).equals(ByteAmount.fromBytes(1)));
    assertTrue(ByteAmount.fromBytes(0).equals(ByteAmount.ZERO));
    assertTrue(ByteAmount.ZERO.equals(ByteAmount.fromBytes(0)));
    assertFalse(ByteAmount.fromBytes(0).equals(null));
    assertFalse(ByteAmount.fromBytes(2).equals(null));
    assertFalse(ByteAmount.fromBytes(2).equals("foo"));
  }

  @Test
  public void testCompare() {
    assertTrue(ByteAmount.fromBytes(0).compareTo(ByteAmount.fromBytes(1)) < 0);
    assertTrue(ByteAmount.fromBytes(1).compareTo(ByteAmount.fromBytes(1)) == 0);
    assertTrue(ByteAmount.fromBytes(2).compareTo(ByteAmount.fromBytes(1)) > 0);
    assertTrue(ByteAmount.fromBytes(0).compareTo(ByteAmount.ZERO) == 0);
    assertTrue(ByteAmount.ZERO.compareTo(ByteAmount.fromBytes(0)) == 0);
  }

  @Test(expected = NullPointerException.class)
  public void testCompareNull() {
    ByteAmount.fromBytes(0).compareTo(null);
  }

  @Test
  public void testToString() {
    assertEquals("ByteAmount{0 bytes}",
        ByteAmount.fromBytes(0).toString());
    assertEquals("ByteAmount{512.0 KB (524287 bytes)}",
        ByteAmount.fromBytes((MB / 2) - 1).toString());
    assertEquals("ByteAmount{0.5 MB (524289 bytes)}",
        ByteAmount.fromBytes((MB / 2) + 1).toString());
    assertEquals("ByteAmount{1.0 MB (1048575 bytes)}",
        ByteAmount.fromBytes(MB - 1).toString());
    assertEquals("ByteAmount{1.0 MB (1048576 bytes)}",
        ByteAmount.fromBytes(MB).toString());
    assertEquals("ByteAmount{1.0 GB (1073741823 bytes)}",
        ByteAmount.fromBytes(GB - 1).toString());
    assertEquals("ByteAmount{1.0 GB (1073741824 bytes)}",
        ByteAmount.fromBytes(GB).toString());
    assertEquals("ByteAmount{1.1 GB (1181116006 bytes)}",
        ByteAmount.fromBytes(GB).increaseBy(10).toString());
    assertEquals("ByteAmount{2.0 GB (2147483648 bytes)}",
        ByteAmount.fromBytes(2 * GB).toString());
    assertEquals("ByteAmount{2.0 GB (2147483647 bytes)}",
        ByteAmount.fromBytes((2 * GB) - 1).toString());
    assertEquals("ByteAmount{8589934592.0 GB (9223372036854775807 bytes)}",
        ByteAmount.fromBytes(Long.MAX_VALUE).toString());
  }
}
