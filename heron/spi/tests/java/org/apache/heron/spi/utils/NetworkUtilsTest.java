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

package org.apache.heron.spi.utils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.net.HttpURLConnection;
import java.net.InetSocketAddress;
import java.net.URL;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import org.apache.heron.common.basics.Pair;
import org.apache.heron.common.basics.SysUtils;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("jdk.internal.reflect.*")
@PrepareForTest({SysUtils.class, NetworkUtils.class, ShellUtils.class})
public class NetworkUtilsTest {
  private static final Logger LOG = Logger.getLogger(NetworkUtilsTest.class.getName());

  @Test
  public void testSendHttpResponse() throws Exception {
    HttpExchange exchange = Mockito.mock(HttpExchange.class);
    Mockito.doNothing().when(exchange).sendResponseHeaders(Matchers.anyInt(), Matchers.anyLong());

    OutputStream os = Mockito.mock(OutputStream.class);
    Mockito.doReturn(os).when(exchange).getResponseBody();
    Mockito.doNothing().when(os).write(Matchers.any(byte[].class));
    Mockito.doNothing().when(os).close();

    Assert.assertTrue(NetworkUtils.sendHttpResponse(exchange, new byte[0]));
    Mockito.verify(exchange).getResponseBody();
    Mockito.verify(os, Mockito.atLeastOnce()).write(Matchers.any(byte[].class));
    Mockito.verify(os, Mockito.atLeastOnce()).close();
  }

  @Test
  public void testSendHttpResponseFail() throws Exception {
    HttpExchange exchange = Mockito.mock(HttpExchange.class);
    Mockito.doThrow(new IOException("Designed IO exception for testing")).
        when(exchange).sendResponseHeaders(Matchers.anyInt(), Matchers.anyLong());
    Assert.assertFalse(NetworkUtils.sendHttpResponse(exchange, new byte[0]));
    Mockito.verify(exchange, Mockito.never()).getResponseBody();


    Mockito.doNothing().
        when(exchange).sendResponseHeaders(Matchers.anyInt(), Matchers.anyLong());
    OutputStream os = Mockito.mock(OutputStream.class);
    Mockito.doReturn(os).when(exchange).getResponseBody();

    Mockito.doThrow(new IOException("Designed IO exception for testing")).
        when(os).write(Matchers.any(byte[].class));
    Assert.assertFalse(NetworkUtils.sendHttpResponse(exchange, new byte[0]));
    Mockito.verify(os, Mockito.atLeastOnce()).close();

    Mockito.doNothing().when(os).write(Matchers.any(byte[].class));
    Mockito.doThrow(new IOException("Designed IO exception for testing"))
        .when(os).close();
    Assert.assertFalse(NetworkUtils.sendHttpResponse(exchange, new byte[0]));
  }

  @Test
  public void testReadHttpRequestBody() throws Exception {
    byte[] expectedBytes = "TO READ".getBytes();
    InputStream is = Mockito.spy(new ByteArrayInputStream(expectedBytes));

    HttpExchange exchange = Mockito.mock(HttpExchange.class);
    Headers headers = Mockito.mock(Headers.class);
    Mockito.doReturn(Integer.toString(expectedBytes.length)).
        when(headers).getFirst(Matchers.anyString());

    Mockito.doReturn(headers).when(exchange).getRequestHeaders();
    Mockito.doReturn(is).when(exchange).getRequestBody();

    Assert.assertArrayEquals(expectedBytes, NetworkUtils.readHttpRequestBody(exchange));
    Mockito.verify(is, Mockito.atLeastOnce()).close();
  }

  @Test
  public void testReadHttpRequestBodyFail() throws Exception {
    HttpExchange exchange = Mockito.mock(HttpExchange.class);
    Headers headers = Mockito.mock(Headers.class);
    Mockito.doReturn(headers).when(exchange).getRequestHeaders();

    Mockito.doReturn("-1").
        when(headers).getFirst(Matchers.anyString());
    Assert.assertArrayEquals(new byte[0], NetworkUtils.readHttpRequestBody(exchange));

    Mockito.doReturn("10").
        when(headers).getFirst(Matchers.anyString());
    InputStream inputStream = Mockito.mock(InputStream.class);
    Mockito.doReturn(inputStream).when(exchange).getRequestBody();
    Mockito.doThrow(new IOException("Designed IO exception for testing"))
        .when(inputStream).read(Matchers.any(byte[].class), Matchers.anyInt(), Matchers.anyInt());
    Assert.assertArrayEquals(new byte[0], NetworkUtils.readHttpRequestBody(exchange));
    Mockito.verify(inputStream, Mockito.atLeastOnce()).close();
  }

  @Test
  public void testSendHttpPostRequest() throws Exception {
    URL url = new URL("http://");
    int dataLength = 100;

    HttpURLConnection connection = Mockito.spy((HttpURLConnection) url.openConnection());

    OutputStream os = Mockito.mock(OutputStream.class);

    Mockito.doReturn(os).when(connection).getOutputStream();

    byte[] data = new byte[dataLength];
    Assert.assertTrue(NetworkUtils.sendHttpPostRequest(connection,
        NetworkUtils.URL_ENCODE_TYPE, data));

    Assert.assertEquals("POST", connection.getRequestMethod());
    Assert.assertEquals("application/x-www-form-urlencoded",
        connection.getRequestProperty("Content-Type"));

    Assert.assertEquals(false, connection.getUseCaches());

    Assert.assertEquals(true, connection.getDoOutput());

    connection.disconnect();
  }

  @Test
  public void testSendHttpPostRequestFail() throws Exception {
    URL url = new URL("http://");

    HttpURLConnection connection = Mockito.spy((HttpURLConnection) url.openConnection());
    Mockito.doThrow(new IOException("Designed IO exception for testing")).
        when(connection).getOutputStream();

    Assert.assertFalse(NetworkUtils.sendHttpPostRequest(connection,
        NetworkUtils.URL_ENCODE_TYPE, new byte[1]));

    connection.disconnect();
  }

  @Test
  public void testReadHttpResponseFail() throws Exception {
    HttpURLConnection connection = Mockito.mock(HttpURLConnection.class);

    // Unable to read response due to wrong response content length
    Mockito.doReturn(HttpURLConnection.HTTP_OK).when(connection).getResponseCode();
    Mockito.doReturn(-1).when(connection).getContentLength();
    Assert.assertArrayEquals(new byte[0], NetworkUtils.readHttpResponse(connection));

    Mockito.doThrow(new IOException("Designed IO exception for testing")).
        when(connection).getResponseCode();
    Assert.assertArrayEquals(new byte[0], NetworkUtils.readHttpResponse(connection));
  }

  @Test
  public void testReadHttpResponse() throws Exception {
    String expectedResponseString = "Hello World!";
    byte[] expectedBytes = expectedResponseString.getBytes();
    HttpURLConnection connection = Mockito.mock(HttpURLConnection.class);

    // every response code should return a body if one exists
    Mockito.doReturn(expectedBytes.length).when(connection).getContentLength();
    for (Integer responseCode : getAllHttpCodes()) {
      Mockito.doReturn(responseCode).when(connection).getResponseCode();

      InputStream is = new ByteArrayInputStream(expectedBytes);
      Mockito.doReturn(is).when(connection).getInputStream();
      Assert.assertArrayEquals(expectedBytes, NetworkUtils.readHttpResponse(connection));
    }
  }

  /**
   * Test establishSSHTunnelIfNeeded()
   */
  @Test
  public void testEstablishSSHTunnelIfNeeded() throws Exception {
    // Mock host to verified
    String mockHost = "host0";
    int mockPort = 9049;
    String mockEndpoint = String.format("%s:%d", mockHost, mockPort);
    InetSocketAddress mockAddr = NetworkUtils.getInetSocketAddress(mockEndpoint);

    int mockFreePort = 9519;

    String tunnelHost = "tunnelHost";
    Duration timeout = Duration.ofMillis(-1);
    int retryCount = -1;
    Duration retryInterval = Duration.ofMillis(-1);
    int verifyCount = -1;
    NetworkUtils.TunnelConfig tunnelConfig = new NetworkUtils.TunnelConfig(
        true, tunnelHost, timeout, retryCount, retryInterval, verifyCount);

    // Can reach directly, no need to ssh tunnel
    PowerMockito.spy(NetworkUtils.class);
    PowerMockito.doReturn(true).when(NetworkUtils.class, "isLocationReachable",
        Mockito.eq(mockAddr), Mockito.eq(timeout), Mockito.anyInt(), Mockito.eq(retryInterval));

    Pair<InetSocketAddress, Process> ret =
        NetworkUtils.establishSSHTunnelIfNeeded(NetworkUtils.getInetSocketAddress(mockEndpoint),
            tunnelConfig, NetworkUtils.TunnelType.PORT_FORWARD);
    Assert.assertEquals(mockHost, ret.first.getHostName());
    Assert.assertEquals(mockPort, ret.first.getPort());
    Assert.assertEquals(mockEndpoint, ret.first.toString());
    Assert.assertNull(ret.second);

    // Can not reach directly, basic setup
    PowerMockito.doReturn(false).when(NetworkUtils.class, "isLocationReachable",
        Mockito.eq(mockAddr), Mockito.eq(timeout), Mockito.anyInt(), Mockito.eq(retryInterval));
    PowerMockito.spy(SysUtils.class);
    PowerMockito.doReturn(mockFreePort).when(SysUtils.class, "getFreePort");
    Process process = Mockito.mock(Process.class);
    Mockito.doReturn(true).when(process).isAlive();

    // Can not reach directly, failed to establish ssh tunnel either
    PowerMockito.spy(ShellUtils.class);
    PowerMockito.doReturn(process).when(ShellUtils.class, "establishSSHTunnelProcess",
        Mockito.anyString(), Mockito.anyInt(), Mockito.anyString(),
        Mockito.anyInt());

    InetSocketAddress newAddress =
        NetworkUtils.getInetSocketAddress(
            String.format("%s:%d", NetworkUtils.LOCAL_HOST, mockFreePort));

    PowerMockito.doReturn(false).when(NetworkUtils.class, "isLocationReachable",
        Mockito.eq(newAddress), Mockito.eq(timeout), Mockito.anyInt(), Mockito.eq(retryInterval));
    ret = NetworkUtils.establishSSHTunnelIfNeeded(NetworkUtils.getInetSocketAddress(mockEndpoint),
            tunnelConfig, NetworkUtils.TunnelType.PORT_FORWARD);
    Assert.assertNull(ret.first);
    Assert.assertNull(ret.second);

    // Can not reach directly, but can establish ssh tunnel to reach the destination
    PowerMockito.doReturn(true).when(NetworkUtils.class, "isLocationReachable",
        Mockito.eq(newAddress), Mockito.eq(timeout), Mockito.anyInt(), Mockito.eq(retryInterval));
    ret = NetworkUtils.establishSSHTunnelIfNeeded(NetworkUtils.getInetSocketAddress(mockEndpoint),
        tunnelConfig, NetworkUtils.TunnelType.PORT_FORWARD);
    Assert.assertEquals(NetworkUtils.LOCAL_HOST, ret.first.getHostName());
    Assert.assertEquals(mockFreePort, ret.first.getPort());
    Assert.assertEquals(process, ret.second);
  }

  @Test
  public void testGetInetSocketAddress() throws Exception {
    String host = "host";
    int port = 999;
    String endpoint = String.format("%s:%d", host, port);
    InetSocketAddress address = NetworkUtils.getInetSocketAddress(endpoint);
    Assert.assertEquals(host, address.getHostString());
    Assert.assertEquals(port, address.getPort());
    Assert.assertEquals(endpoint, address.toString());
  }

  private List<Integer> getAllHttpCodes() throws IllegalAccessException {
    Field[] declaredFields = HttpURLConnection.class.getDeclaredFields();
    List<Integer> responseCodes = new ArrayList<>();
    for (Field field : declaredFields) {
      if (Modifier.isStatic(field.getModifiers())) {
        if (field.getName().contains("HTTP_")) {
          responseCodes.add(field.getInt(null));
        }
      }
    }
    return responseCodes;
  }
}
