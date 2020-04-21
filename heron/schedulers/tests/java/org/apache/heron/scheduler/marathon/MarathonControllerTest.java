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

package org.apache.heron.scheduler.marathon;

import java.net.HttpURLConnection;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.heron.spi.utils.NetworkUtils;


@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest(NetworkUtils.class)
public class MarathonControllerTest {
  private static final String MARATHON_URI = "http://marathon.uri:8080";
  private static final String MARATHON_AUTH_TOKEN = "a_token";
  private static final String TOPOLOGY_NAME = "topology_name";
  private static final boolean IS_VERBOSE = true;

  private static MarathonController controller;

  @Before
  public void setUp() throws Exception {
    controller = Mockito.spy(new MarathonController(MARATHON_URI, MARATHON_AUTH_TOKEN,
                                                    TOPOLOGY_NAME, IS_VERBOSE));
  }

  @After
  public void after() throws Exception {
  }

  @BeforeClass
  public static void beforeClass() throws Exception {
  }

  @AfterClass
  public static void afterClass() throws Exception {
  }

  /***
   * Test MarathonController's killTopology method
   * @throws Exception
   */
  @Test
  public void testKillTopology() throws Exception {
    HttpURLConnection httpURLConnection = Mockito.mock(HttpURLConnection.class);

    // Failed to get connection
    PowerMockito.spy(NetworkUtils.class);
    PowerMockito.doReturn(httpURLConnection)
        .when(NetworkUtils.class, "getHttpConnection", Mockito.anyString());
    Assert.assertFalse(controller.killTopology());
    PowerMockito.verifyStatic();
    NetworkUtils.getHttpConnection(Mockito.anyString());

    // Failed to send request
    PowerMockito.spy(NetworkUtils.class);
    PowerMockito.doReturn(httpURLConnection)
        .when(NetworkUtils.class, "getHttpConnection", Mockito.anyString());
    PowerMockito.doReturn(false)
        .when(NetworkUtils.class, "sendHttpDeleteRequest", Mockito.any(HttpURLConnection.class));
    Assert.assertFalse(controller.killTopology());
    PowerMockito.verifyStatic();
    NetworkUtils.getHttpConnection(Mockito.anyString());
    NetworkUtils.sendHttpDeleteRequest(Mockito.any(HttpURLConnection.class));

    // Failed to get response
    PowerMockito.spy(NetworkUtils.class);
    PowerMockito.doReturn(httpURLConnection)
        .when(NetworkUtils.class, "getHttpConnection", Mockito.anyString());
    PowerMockito.doReturn(true)
        .when(NetworkUtils.class, "sendHttpDeleteRequest", Mockito.any(HttpURLConnection.class));
    PowerMockito.doReturn(false)
        .when(NetworkUtils.class, "checkHttpResponseCode",
            Mockito.any(HttpURLConnection.class), Mockito.anyInt());
    Assert.assertFalse(controller.killTopology());
    PowerMockito.verifyStatic();
    NetworkUtils.getHttpConnection(Mockito.anyString());
    NetworkUtils.sendHttpDeleteRequest(Mockito.any(HttpURLConnection.class));
    NetworkUtils.checkHttpResponseCode(Mockito.any(HttpURLConnection.class), Mockito.anyInt());

    // Failed authentication
    PowerMockito.spy(NetworkUtils.class);
    PowerMockito.doReturn(httpURLConnection)
        .when(NetworkUtils.class, "getHttpConnection", Mockito.anyString());
    PowerMockito.doReturn(true)
        .when(NetworkUtils.class, "sendHttpPostRequest",
            Mockito.any(HttpURLConnection.class),
            Mockito.anyString(),
            Mockito.any(byte[].class));
    PowerMockito.doReturn(false)
        .when(NetworkUtils.class, "checkHttpResponseCode",
            Mockito.any(HttpURLConnection.class), Mockito.anyInt());
    PowerMockito.doReturn(true)
        .when(NetworkUtils.class, "checkHttpResponseCode",
            httpURLConnection, HttpURLConnection.HTTP_UNAUTHORIZED);
    Assert.assertFalse(controller.killTopology());
    PowerMockito.verifyStatic();
    NetworkUtils.getHttpConnection(Mockito.anyString());
    NetworkUtils.sendHttpPostRequest(
        Mockito.any(HttpURLConnection.class),
        Mockito.anyString(),
        Mockito.any(byte[].class));
    NetworkUtils.checkHttpResponseCode(Mockito.any(HttpURLConnection.class), Mockito.anyInt());
    NetworkUtils.checkHttpResponseCode(httpURLConnection, HttpURLConnection.HTTP_UNAUTHORIZED);

    // Success
    PowerMockito.spy(NetworkUtils.class);
    PowerMockito.doReturn(httpURLConnection)
        .when(NetworkUtils.class, "getHttpConnection", Mockito.anyString());
    PowerMockito.doReturn(true)
        .when(NetworkUtils.class, "sendHttpDeleteRequest", Mockito.any(HttpURLConnection.class));
    PowerMockito.doReturn(true)
        .when(NetworkUtils.class, "checkHttpResponseCode",
            Mockito.any(HttpURLConnection.class), Mockito.anyInt());
    Assert.assertTrue(controller.killTopology());
    PowerMockito.verifyStatic();
    NetworkUtils.getHttpConnection(Mockito.anyString());
    NetworkUtils.sendHttpDeleteRequest(Mockito.any(HttpURLConnection.class));
    NetworkUtils.checkHttpResponseCode(Mockito.any(HttpURLConnection.class), Mockito.anyInt());
  }

  /***
   * Test MarathonController's restartApp method
   * @throws Exception
   */
  @Test
  public void testRestartApp() throws Exception {
    HttpURLConnection httpURLConnection = Mockito.mock(HttpURLConnection.class);
    int appId = 0;

    // Failed to get connection
    PowerMockito.spy(NetworkUtils.class);
    PowerMockito.doReturn(httpURLConnection)
        .when(NetworkUtils.class, "getHttpConnection", Mockito.anyString());
    Assert.assertFalse(controller.restartApp(appId));
    PowerMockito.verifyStatic();
    NetworkUtils.getHttpConnection(Mockito.anyString());

    // Failed to send request
    PowerMockito.spy(NetworkUtils.class);
    PowerMockito.doReturn(httpURLConnection)
        .when(NetworkUtils.class, "getHttpConnection", Mockito.anyString());
    PowerMockito.doReturn(false)
        .when(NetworkUtils.class, "sendHttpPostRequest",
            Mockito.any(HttpURLConnection.class),
            Mockito.anyString(),
            Mockito.any(byte[].class));
    Assert.assertFalse(controller.restartApp(appId));
    PowerMockito.verifyStatic();
    NetworkUtils.getHttpConnection(Mockito.anyString());
    NetworkUtils.sendHttpPostRequest(
        Mockito.any(HttpURLConnection.class),
        Mockito.anyString(),
        Mockito.any(byte[].class));

    // Failed to get response
    PowerMockito.spy(NetworkUtils.class);
    PowerMockito.doReturn(httpURLConnection)
        .when(NetworkUtils.class, "getHttpConnection", Mockito.anyString());
    PowerMockito.doReturn(true)
        .when(NetworkUtils.class, "sendHttpPostRequest",
            Mockito.any(HttpURLConnection.class),
            Mockito.anyString(),
            Mockito.any(byte[].class));
    PowerMockito.doReturn(false)
        .when(NetworkUtils.class, "checkHttpResponseCode",
            Mockito.any(HttpURLConnection.class), Mockito.anyInt());
    Assert.assertFalse(controller.restartApp(appId));
    PowerMockito.verifyStatic();
    NetworkUtils.getHttpConnection(Mockito.anyString());
    NetworkUtils.sendHttpPostRequest(
        Mockito.any(HttpURLConnection.class),
        Mockito.anyString(),
        Mockito.any(byte[].class));
    NetworkUtils.checkHttpResponseCode(Mockito.any(HttpURLConnection.class), Mockito.anyInt());

    // Failed authentication
    PowerMockito.spy(NetworkUtils.class);
    PowerMockito.doReturn(httpURLConnection)
        .when(NetworkUtils.class, "getHttpConnection", Mockito.anyString());
    PowerMockito.doReturn(true)
        .when(NetworkUtils.class, "sendHttpPostRequest",
            Mockito.any(HttpURLConnection.class),
            Mockito.anyString(),
            Mockito.any(byte[].class));
    PowerMockito.doReturn(false)
        .when(NetworkUtils.class, "checkHttpResponseCode",
            Mockito.any(HttpURLConnection.class), Mockito.anyInt());
    PowerMockito.doReturn(true)
        .when(NetworkUtils.class, "checkHttpResponseCode",
            httpURLConnection, HttpURLConnection.HTTP_UNAUTHORIZED);
    Assert.assertFalse(controller.restartApp(appId));
    PowerMockito.verifyStatic();
    NetworkUtils.getHttpConnection(Mockito.anyString());
    NetworkUtils.sendHttpPostRequest(
        Mockito.any(HttpURLConnection.class),
        Mockito.anyString(),
        Mockito.any(byte[].class));
    NetworkUtils.checkHttpResponseCode(Mockito.any(HttpURLConnection.class), Mockito.anyInt());
    NetworkUtils.checkHttpResponseCode(httpURLConnection, HttpURLConnection.HTTP_UNAUTHORIZED);

    // Success
    PowerMockito.spy(NetworkUtils.class);
    PowerMockito.doReturn(httpURLConnection)
        .when(NetworkUtils.class, "getHttpConnection", Mockito.anyString());
    PowerMockito.doReturn(true)
        .when(NetworkUtils.class, "sendHttpPostRequest",
            Mockito.any(HttpURLConnection.class),
            Mockito.anyString(),
            Mockito.any(byte[].class));
    PowerMockito.doReturn(true)
        .when(NetworkUtils.class, "checkHttpResponseCode",
            Mockito.any(HttpURLConnection.class), Mockito.anyInt());
    Assert.assertTrue(controller.restartApp(appId));
    PowerMockito.verifyStatic();
    NetworkUtils.getHttpConnection(Mockito.anyString());
    NetworkUtils.sendHttpPostRequest(
        Mockito.any(HttpURLConnection.class),
        Mockito.anyString(),
        Mockito.any(byte[].class));
    NetworkUtils.checkHttpResponseCode(Mockito.any(HttpURLConnection.class), Mockito.anyInt());
  }

  /***
   * Test MarathonController's submitTopology method
   * @throws Exception
   */
  @Test
  public void testSubmitTopology() throws Exception {
    HttpURLConnection httpURLConnection = Mockito.mock(HttpURLConnection.class);
    final String appConf = "{app: conf}";

    // Failed to get connection
    PowerMockito.spy(NetworkUtils.class);
    PowerMockito.doReturn(httpURLConnection)
        .when(NetworkUtils.class, "getHttpConnection", Mockito.anyString());
    Assert.assertFalse(controller.submitTopology(Mockito.anyString()));
    PowerMockito.verifyStatic();
    NetworkUtils.getHttpConnection(Mockito.anyString());

    // Failed to send request
    PowerMockito.spy(NetworkUtils.class);
    PowerMockito.doReturn(httpURLConnection)
        .when(NetworkUtils.class, "getHttpConnection", Mockito.anyString());
    PowerMockito.doReturn(false)
        .when(NetworkUtils.class, "sendHttpPostRequest",
            Mockito.any(HttpURLConnection.class),
            Mockito.anyString(),
            Mockito.any(byte[].class));
    Assert.assertFalse(controller.submitTopology(appConf));
    PowerMockito.verifyStatic();
    NetworkUtils.getHttpConnection(Mockito.anyString());
    NetworkUtils.sendHttpPostRequest(
        Mockito.any(HttpURLConnection.class),
        Mockito.anyString(),
        Mockito.any(byte[].class));

    // Failed to get response
    PowerMockito.spy(NetworkUtils.class);
    PowerMockito.doReturn(httpURLConnection)
        .when(NetworkUtils.class, "getHttpConnection", Mockito.anyString());
    PowerMockito.doReturn(true)
        .when(NetworkUtils.class, "sendHttpPostRequest",
            Mockito.any(HttpURLConnection.class),
            Mockito.anyString(),
            Mockito.any(byte[].class));
    PowerMockito.doReturn(false)
        .when(NetworkUtils.class, "checkHttpResponseCode",
            Mockito.any(HttpURLConnection.class), Mockito.anyInt());
    Assert.assertFalse(controller.submitTopology(appConf));
    PowerMockito.verifyStatic();
    NetworkUtils.getHttpConnection(Mockito.anyString());
    NetworkUtils.sendHttpPostRequest(
        Mockito.any(HttpURLConnection.class),
        Mockito.anyString(),
        Mockito.any(byte[].class));
    NetworkUtils.checkHttpResponseCode(Mockito.any(HttpURLConnection.class), Mockito.anyInt());

    // Failed authentication
    PowerMockito.spy(NetworkUtils.class);
    PowerMockito.doReturn(httpURLConnection)
        .when(NetworkUtils.class, "getHttpConnection", Mockito.anyString());
    PowerMockito.doReturn(true)
        .when(NetworkUtils.class, "sendHttpPostRequest",
            Mockito.any(HttpURLConnection.class),
            Mockito.anyString(),
            Mockito.any(byte[].class));
    PowerMockito.doReturn(false)
        .when(NetworkUtils.class, "checkHttpResponseCode",
            Mockito.any(HttpURLConnection.class), Mockito.anyInt());
    PowerMockito.doReturn(true)
        .when(NetworkUtils.class, "checkHttpResponseCode",
            httpURLConnection, HttpURLConnection.HTTP_UNAUTHORIZED);
    Assert.assertFalse(controller.submitTopology(appConf));
    PowerMockito.verifyStatic();
    NetworkUtils.getHttpConnection(Mockito.anyString());
    NetworkUtils.sendHttpPostRequest(
        Mockito.any(HttpURLConnection.class),
        Mockito.anyString(),
        Mockito.any(byte[].class));
    NetworkUtils.checkHttpResponseCode(Mockito.any(HttpURLConnection.class), Mockito.anyInt());
    NetworkUtils.checkHttpResponseCode(httpURLConnection, HttpURLConnection.HTTP_UNAUTHORIZED);


    // Success
    PowerMockito.spy(NetworkUtils.class);
    PowerMockito.doReturn(httpURLConnection)
        .when(NetworkUtils.class, "getHttpConnection", Mockito.anyString());
    PowerMockito.doReturn(true)
        .when(NetworkUtils.class, "sendHttpPostRequest",
            Mockito.any(HttpURLConnection.class),
            Mockito.anyString(),
            Mockito.any(byte[].class));
    PowerMockito.doReturn(true)
        .when(NetworkUtils.class, "checkHttpResponseCode",
            Mockito.any(HttpURLConnection.class), Mockito.anyInt());
    Assert.assertTrue(controller.submitTopology(appConf));
    PowerMockito.verifyStatic();
    NetworkUtils.getHttpConnection(Mockito.anyString());
    NetworkUtils.sendHttpPostRequest(
        Mockito.any(HttpURLConnection.class),
        Mockito.anyString(),
        Mockito.any(byte[].class));
    NetworkUtils.checkHttpResponseCode(Mockito.any(HttpURLConnection.class), Mockito.anyInt());
  }
}
