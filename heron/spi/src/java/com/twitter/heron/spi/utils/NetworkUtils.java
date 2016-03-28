package com.twitter.heron.spi.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ProtocolException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.common.util.concurrent.ListenableFuture;
import com.sun.net.httpserver.HttpExchange;

import com.twitter.heron.proto.system.Common;

/**
 * Utilities related to network.
 */
public class NetworkUtils {
  public static final String CONTENT_LENGTH = "Content-Length";
  public static final String CONTENT_TYPE = "Content-Type";

  private static final Logger LOG = Logger.getLogger(NetworkUtils.class.getName());

  /**
   * Waits for ListenableFuture to terminate. Cancels on timeout
   */
  public static <V> V awaitResult(ListenableFuture<V> future, int time, TimeUnit unit) {
    try {
      return future.get(time, unit);
    } catch (InterruptedException | TimeoutException | ExecutionException e) {
      LOG.log(Level.SEVERE, "Exception processing future ", e);
      future.cancel(true);
      return null;
    }
  }

  /**
   * Get available port.
   *
   * @return available port.
   */
  public static int getFreePort() {
    try (ServerSocket socket = new ServerSocket(0)) {
      int port = socket.getLocalPort();
      socket.close();
      return port;
    } catch (IOException ioe) {
      return -1;
    }
  }

  public static Common.Status getHeronStatus(boolean isOK) {
    Common.Status.Builder status = Common.Status.newBuilder();
    if (isOK) {
      status.setStatus(Common.StatusCode.OK);
    } else {
      status.setStatus(Common.StatusCode.NOTOK);
    }
    return status.build();
  }
}
