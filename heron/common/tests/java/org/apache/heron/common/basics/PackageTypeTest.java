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

import org.junit.Assert;
import org.junit.Test;

/**
 * PackageTypeTester.
 */
public class PackageTypeTest {
   /**
   * Method: isOriginalPackageJar(String packageFilename)
   */
  @Test
  public void testPackageType() {
    String jarFile = "a.jar";
    Assert.assertEquals(PackageType.getPackageType(jarFile), PackageType.JAR);

    String tarFile = "b.tar";
    Assert.assertEquals(PackageType.getPackageType(tarFile), PackageType.TAR);

    String pexFile = "c.pex";
    Assert.assertEquals(PackageType.getPackageType(pexFile), PackageType.PEX);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testUnknownPackageType() {
    String txtFile = "a.txt";
    PackageType.getPackageType(txtFile);
  }
}
