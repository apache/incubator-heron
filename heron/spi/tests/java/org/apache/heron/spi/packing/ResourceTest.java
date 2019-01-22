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

package org.apache.heron.spi.packing;

import org.junit.Assert;
import org.junit.Test;

import org.apache.heron.common.basics.ByteAmount;

public class ResourceTest {
  private static final ByteAmount ZERO = ByteAmount.ZERO;
  private static final ByteAmount ONE = ByteAmount.fromBytes(1);
  private static final ByteAmount TWO = ByteAmount.fromBytes(2);

  @Test
  public void testSubtract() {
    Resource firstResource = new Resource(2, TWO, TWO);
    Resource secondResource = new Resource(1, ONE, ZERO);
    Resource additionalResource = firstResource.subtractAbsolute(secondResource);
    Assert.assertEquals(1, (long) additionalResource.getCpu());
    Assert.assertEquals(ONE, additionalResource.getRam());
    Assert.assertEquals(TWO, additionalResource.getDisk());
  }

  @Test
  public void testSubtractNegative() {
    Resource firstResource = new Resource(2, TWO, TWO);
    Resource secondResource = new Resource(4, ByteAmount.fromBytes(3), ONE);
    Resource additionalResource = firstResource.subtractAbsolute(secondResource);
    Assert.assertEquals(0, (long) additionalResource.getCpu());
    Assert.assertEquals(ZERO, additionalResource.getRam());
    Assert.assertEquals(ONE, additionalResource.getDisk());
  }

  @Test
  public void testPlus() {
    Resource firstResource = new Resource(2, TWO, TWO);
    Resource secondResource = new Resource(5, ByteAmount.fromBytes(3), ONE);
    Resource totalResource = firstResource.plus(secondResource);
    Assert.assertEquals(7, (long) totalResource.getCpu());
    Assert.assertEquals(ByteAmount.fromBytes(5), totalResource.getRam());
    Assert.assertEquals(ByteAmount.fromBytes(3), totalResource.getDisk());
  }

  @Test
  public void testDivideBy() {
    Resource firstResource = new Resource(12, ByteAmount.fromBytes(13), ByteAmount.fromBytes(15));
    Resource secondResource = new Resource(2, TWO, TWO);
    double result = firstResource.divideBy(secondResource);
    Assert.assertEquals(8, (long) result);
  }
}
