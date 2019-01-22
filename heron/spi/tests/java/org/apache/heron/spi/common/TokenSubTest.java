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

package org.apache.heron.spi.common;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TokenSubTest {

  private static String substitute(String pathString) {
    Config config = Config.newBuilder()
        .put(Key.BUILD_HOST, "build_host")
        .put(Key.HERON_LIB, "/some/lib/dir")
        .put(Key.HERON_HOME, "/usr/local/heron")
        .put(Key.TOPOLOGY_NAME, "topology_name")
        .build();
    return TokenSub.substitute(config, pathString);
  }

  /**
   * Test if the ${HERON_HOME} variable can be substituted
   */
  @Test
  public void testHeronHome() {
    // check no occurrence
    assertEquals("./bin", substitute("./bin"));

    // check a single substitution at the beginning
    assertEquals("/usr/local/heron/bin", substitute("${HERON_HOME}/bin"));

    // check a single substitution at the beginning with relative path
    assertEquals("./usr/local/heron/bin", substitute("./${HERON_HOME}/bin"));

    // check a single substitution at the end
    assertEquals("/bin/usr/local/heron", substitute("/bin/${HERON_HOME}"));

    // check a single substitution at the end with relative path
    assertEquals("./bin/usr/local/heron", substitute("./bin/${HERON_HOME}"));

    // check a single substitution in the middle
    assertEquals("/bin/usr/local/heron/etc", substitute("/bin/${HERON_HOME}/etc"));

    // check a single substitution in the middle with relative path
    assertEquals("./bin/usr/local/heron/etc", substitute("./bin/${HERON_HOME}/etc"));
  }

  @Test
  public void testURL() {
    assertTrue(TokenSub.isURL("file:///users/john/afile.txt"));
    assertFalse(TokenSub.isURL("/users/john/afile.txt"));
    assertTrue(TokenSub.isURL("https://gotoanywebsite.net/afile.html"));
    assertFalse(TokenSub.isURL("https//gotoanywebsite.net//afile.html"));
  }

  @Test
  public void testToken() {
    assertTrue(TokenSub.isToken("${FOO_BAR}"));
    assertTrue(TokenSub.isToken("${FOO}"));
    assertFalse(TokenSub.isToken("x${FOO}"));
    assertFalse(TokenSub.isToken("${FOO}x"));
    assertFalse(TokenSub.isToken("${}"));
    assertFalse(TokenSub.isToken("$FOO"));
    assertFalse(TokenSub.isToken("foo"));
  }

  @Test
  public void testArbitraryToken() {
    assertEquals("/some/lib/dir/some/path", substitute("${HERON_LIB}/some/path"));
    assertEquals("./bin/build_host/etc", substitute("./bin/${BUILD_HOST}/etc"));
    assertEquals("./bin/topology_name/etc", substitute("./bin/${TOPOLOGY}/etc"));
    assertEquals("./bin/topology_name/etc", substitute("./bin/${TOPOLOGY_NAME}/etc"));
  }
}
