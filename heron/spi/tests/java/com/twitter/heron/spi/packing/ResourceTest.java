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

package com.twitter.heron.spi.packing;

import org.junit.Assert;
import org.junit.Test;

public class ResourceTest {

  @Test
  public void testSubtract() {
    Resource firstResource = new Resource(2, 2, 2);
    Resource secondResource = new Resource(1, 1, 0);
    Resource additionalResource = firstResource.subtractAbsolute(secondResource);
    Assert.assertEquals(1, (long) additionalResource.getCpu());
    Assert.assertEquals(1, additionalResource.getRam());
    Assert.assertEquals(2, additionalResource.getDisk());
  }

  @Test
  public void testSubtractNegative() {
    Resource firstResource = new Resource(2, 2, 2);
    Resource secondResource = new Resource(4, 3, 1);
    Resource additionalResource = firstResource.subtractAbsolute(secondResource);
    Assert.assertEquals(0, (long) additionalResource.getCpu());
    Assert.assertEquals(0, additionalResource.getRam());
    Assert.assertEquals(1, additionalResource.getDisk());
  }

  @Test
  public void testDivideBy() {
    Resource firstResource = new Resource(12, 13, 15);
    Resource secondResource = new Resource(2, 2, 2);
    double result = firstResource.divideBy(secondResource);
    Assert.assertEquals(8, (long) result);
  }
}
