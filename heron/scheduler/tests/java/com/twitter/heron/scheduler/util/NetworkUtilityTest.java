package com.twitter.heron.scheduler.util;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.ServerSocket;
import java.net.SocketException;
import java.net.URL;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.twitter.heron.proto.system.Common;


public class NetworkUtilityTest {
  @Test
  public void testFreePort() {
    int numAttempts = 100;
    // Randomized test
    for (int i = 0; i < numAttempts; ++i) {
      int port = NetworkUtility.getFreePort();
      // verify that port is free
      try {
        new ServerSocket(port).close();
      } catch (SocketException se) {
        Assert.assertTrue("Returned port is not open", false);
      } catch (IOException e) {
      }
    }
  }

  @Test
  public void testSendHttpResponse() throws Exception {
    HttpExchange exchange = Mockito.mock(HttpExchange.class);
    Mockito.doNothing().when(exchange).sendResponseHeaders(Matchers.anyInt(), Matchers.anyLong());

    OutputStream os = Mockito.mock(OutputStream.class);
    Mockito.doReturn(os).when(exchange).getResponseBody();
    Mockito.doNothing().when(os).write(Matchers.any(byte[].class));
    Mockito.doNothing().when(os).close();

    Assert.assertTrue(NetworkUtility.sendHttpResponse(exchange, new byte[0]));
    Mockito.verify(exchange).getResponseBody();
    Mockito.verify(os, Mockito.atLeastOnce()).write(Matchers.any(byte[].class));
    Mockito.verify(os, Mockito.atLeastOnce()).close();
  }

  @Test
  public void testSendHttpResponseFail() throws Exception {
    HttpExchange exchange = Mockito.mock(HttpExchange.class);
    Mockito.doThrow(new IOException("Designed IO exception for testing")).
        when(exchange).sendResponseHeaders(Matchers.anyInt(), Matchers.anyLong());
    Assert.assertFalse(NetworkUtility.sendHttpResponse(exchange, new byte[0]));
    Mockito.verify(exchange, Mockito.never()).getResponseBody();


    Mockito.doNothing().
        when(exchange).sendResponseHeaders(Matchers.anyInt(), Matchers.anyLong());
    OutputStream os = Mockito.mock(OutputStream.class);
    Mockito.doReturn(os).when(exchange).getResponseBody();

    Mockito.doThrow(new IOException("Designed IO exception for testing")).
        when(os).write(Matchers.any(byte[].class));
    Assert.assertFalse(NetworkUtility.sendHttpResponse(exchange, new byte[0]));
    Mockito.verify(os, Mockito.atLeastOnce()).close();

    Mockito.doNothing().when(os).write(Matchers.any(byte[].class));
    Mockito.doThrow(new IOException("Designed IO exception for testing"))
        .when(os).close();
    Assert.assertFalse(NetworkUtility.sendHttpResponse(exchange, new byte[0]));
  }

  @Test
  public void testReadHttpRequestBody() throws Exception {
    byte[] expectedBytes = "TO READ".getBytes();
    InputStream is = Mockito.spy(new ByteArrayInputStream(expectedBytes));

    HttpExchange exchange = Mockito.mock(HttpExchange.class);
    Headers headers = Mockito.mock(Headers.class);
    Mockito.doReturn("" + expectedBytes.length).
        when(headers).getFirst(Matchers.anyString());

    Mockito.doReturn(headers).when(exchange).getRequestHeaders();
    Mockito.doReturn(is).when(exchange).getRequestBody();

    Assert.assertArrayEquals(expectedBytes, NetworkUtility.readHttpRequestBody(exchange));
    Mockito.verify(is, Mockito.atLeastOnce()).close();
  }

  @Test
  public void testReadHttpRequestBodyFail() throws Exception {
    HttpExchange exchange = Mockito.mock(HttpExchange.class);
    Headers headers = Mockito.mock(Headers.class);
    Mockito.doReturn(headers).when(exchange).getRequestHeaders();

    Mockito.doReturn("-1").
        when(headers).getFirst(Matchers.anyString());
    Assert.assertArrayEquals(new byte[0], NetworkUtility.readHttpRequestBody(exchange));

    Mockito.doReturn("10").
        when(headers).getFirst(Matchers.anyString());
    InputStream inputStream = Mockito.mock(InputStream.class);
    Mockito.doReturn(inputStream).when(exchange).getRequestBody();
    Mockito.doThrow(new IOException("Designed IO exception for testing"))
        .when(inputStream).read(Matchers.any(byte[].class), Matchers.anyInt(), Matchers.anyInt());
    Assert.assertArrayEquals(new byte[0], NetworkUtility.readHttpRequestBody(exchange));
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
    Assert.assertTrue(NetworkUtility.sendHttpPostRequest(connection, data));

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

    Assert.assertFalse(NetworkUtility.sendHttpPostRequest(connection, new byte[0]));

    connection.disconnect();
  }

  @Test
  public void testReadHttpResponseFail() throws Exception {
    HttpURLConnection connection = Mockito.mock(HttpURLConnection.class);

    // Unable to read response due to wrong response code
    Mockito.doReturn(HttpURLConnection.HTTP_NOT_FOUND).when(connection).getResponseCode();
    Assert.assertArrayEquals(new byte[0], NetworkUtility.readHttpResponse(connection));

    // Unable to read response due to wrong response content length
    Mockito.doReturn(HttpURLConnection.HTTP_OK).when(connection).getResponseCode();
    Mockito.doReturn(-1).when(connection).getContentLength();
    Assert.assertArrayEquals(new byte[0], NetworkUtility.readHttpResponse(connection));

    Mockito.doThrow(new IOException("Designed IO exception for testing")).
        when(connection).getResponseCode();
    Assert.assertArrayEquals(new byte[0], NetworkUtility.readHttpResponse(connection));
  }

  @Test
  public void testReadHttpResponse() throws Exception {
    String expectedResponseString = "Hello World!";
    byte[] expectedBytes = expectedResponseString.getBytes();
    HttpURLConnection connection = Mockito.mock(HttpURLConnection.class);

    Mockito.doReturn(HttpURLConnection.HTTP_OK).when(connection).getResponseCode();
    Mockito.doReturn(expectedBytes.length).when(connection).getContentLength();

    InputStream is = new ByteArrayInputStream(expectedBytes);
    Mockito.doReturn(is).when(connection).getInputStream();
    Assert.assertArrayEquals(expectedBytes, NetworkUtility.readHttpResponse(connection));
  }

  @Test
  public void testGetHeronStatus() {
    Common.Status okStatus = Common.Status.newBuilder().
        setStatus(Common.StatusCode.OK)
        .build();
    Assert.assertEquals(okStatus, NetworkUtility.getHeronStatus(true));

    Common.Status notOKStatus = Common.Status.newBuilder().
        setStatus(Common.StatusCode.NOTOK)
        .build();
    Assert.assertEquals(notOKStatus, NetworkUtility.getHeronStatus(false));

  }
}