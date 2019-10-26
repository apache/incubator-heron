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

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.Proxy;
import java.net.Socket;
import java.net.URL;
import java.net.UnknownHostException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpExchange;

import org.apache.heron.common.basics.Pair;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.spi.common.Config;

/**
 * Utilities related to network.
 */
public final class NetworkUtils {
  private static final String CONTENT_LENGTH = "Content-Length";
  private static final String CONTENT_TYPE = "Content-Type";
  static final String LOCAL_HOST = "localhost";

  public static final String JSON_TYPE = "application/json";
  public static final String URL_ENCODE_TYPE = "application/x-www-form-urlencoded";

  private static final Logger LOG = Logger.getLogger(NetworkUtils.class.getName());

  public enum TunnelType {
    PORT_FORWARD,
    SOCKS_PROXY,
  }

  public enum HeronSystem {
    STATE_MANAGER("statemgr"),
    SCHEDULER("scheduler");

    private String shortName;

    HeronSystem(String shortName) {
      this.shortName = shortName;
    }

    public String getShortName() {
      return shortName;
    }
  }

  private NetworkUtils() {
  }

  public static class TunnelConfig {
    private static final String IS_TUNNEL_NEEDED = "heron.%s.is.tunnel.needed";
    private static final String TUNNEL_CONNECTION_TIMEOUT_MS =
        "heron.%s.tunnel.connection.timeout.ms";
    private static final String TUNNEL_CONNECTION_RETRY_COUNT =
        "heron.%s.tunnel.connection.retryCount.count";
    private static final String TUNNEL_VERIFY_COUNT = "heron.%s.tunnel.verify.count";
    private static final String TUNNEL_RETRY_INTERVAL_MS = "heron.%s.tunnel.retryCount.interval.ms";
    private static final String TUNNEL_HOST = "heron.%s.tunnel.host";

    private final boolean isTunnelNeeded;
    private final String tunnelHost;
    private final Duration timeout;
    private final int retryCount;
    private final Duration retryInterval;
    private final int verifyCount;

    TunnelConfig(boolean isTunnelNeeded, String tunnelHost, Duration timeout, int retryCount,
                 Duration retryInterval, int verifyCount) {
      this.isTunnelNeeded = isTunnelNeeded;
      this.tunnelHost = tunnelHost;
      this.timeout = timeout;
      this.retryCount = retryCount;
      this.retryInterval = retryInterval;
      this.verifyCount = verifyCount;
    }

    public boolean isTunnelNeeded() {
      return isTunnelNeeded;
    }

    public String getTunnelHost() {
      return tunnelHost;
    }

    private Duration getTimeout() {
      return timeout;
    }

    public int getRetryCount() {
      return retryCount;
    }

    private Duration getRetryInterval() {
      return retryInterval;
    }

    private int getVerifyCount() {
      return verifyCount;
    }

    public static TunnelConfig build(Config config, HeronSystem heronSystem) {
      return new TunnelConfig(
          config.getBooleanValue(getConfigKey(IS_TUNNEL_NEEDED, heronSystem), false),
          config.getStringValue(getConfigKey(TUNNEL_HOST, heronSystem), "no.tunnel.host.specified"),
          config.getDurationValue(getConfigKey(TUNNEL_CONNECTION_TIMEOUT_MS, heronSystem),
              ChronoUnit.MILLIS, Duration.ofSeconds(1)),
          config.getIntegerValue(getConfigKey(TUNNEL_CONNECTION_RETRY_COUNT, heronSystem), 2),
          config.getDurationValue(getConfigKey(TUNNEL_RETRY_INTERVAL_MS, heronSystem),
              ChronoUnit.MILLIS, Duration.ofSeconds(1)),
          config.getIntegerValue(getConfigKey(TUNNEL_VERIFY_COUNT, heronSystem), 10)
      );
    }

    private static String getConfigKey(String keyTemplate, HeronSystem heronSystem) {
      return String.format(keyTemplate, heronSystem.getShortName());
    }
  }

  /**
   * Read the request body of HTTP request from a given HttpExchange
   *
   * @param exchange the HttpExchange to read from
   * @return the byte[] in request body, or new byte[0] if failed to read
   */
  public static byte[] readHttpRequestBody(HttpExchange exchange) {
    // Get the length of request body
    int contentLength = Integer.parseInt(exchange.getRequestHeaders().getFirst(CONTENT_LENGTH));

    if (contentLength <= 0) {
      LOG.log(Level.SEVERE, "Failed to read content length http request body: " + contentLength);
      return new byte[0];
    }
    byte[] requestBody = new byte[contentLength];

    InputStream is = exchange.getRequestBody();
    try {
      int off = 0;
      int bRead = 0;
      while (off != (contentLength - 1)
          && (bRead = is.read(requestBody, off, contentLength - off)) != -1) {
        off += bRead;
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to read http request body: ", e);
      return new byte[0];
    } finally {
      try {
        is.close();
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to close InputStream: ", e);
        return new byte[0];
      }
    }

    return requestBody;
  }

  /**
   * Send a http response with HTTP_OK return code and response body
   *
   * @param isSuccess send back HTTP_OK if it is true, otherwise send back HTTP_UNAVAILABLE
   * @param exchange the HttpExchange to send response
   * @param response the response the sent back in response body
   * @return true if we send the response successfully
   */
  public static boolean sendHttpResponse(
      boolean isSuccess,
      HttpExchange exchange,
      byte[] response) {
    int returnCode = isSuccess ? HttpURLConnection.HTTP_OK : HttpURLConnection.HTTP_UNAVAILABLE;
    try {
      exchange.sendResponseHeaders(returnCode, response.length);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to send response headers: ", e);
      return false;
    }

    OutputStream os = exchange.getResponseBody();
    try {
      os.write(response);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to send http response: ", e);
      return false;
    } finally {
      try {
        os.close();
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to close OutputStream: ", e);
        return false;
      }
    }

    return true;
  }

  public static boolean sendHttpResponse(HttpExchange exchange, byte[] response) {
    return sendHttpResponse(true, exchange, response);
  }

  /**
   * Send Http POST Request to a connection with given data in request body
   *
   * @param connection the connection to send post request to
   * @param contentType the type of the content to be sent
   * @param data the data to send in post request body
   * @return true if success
   */
  public static boolean sendHttpPostRequest(HttpURLConnection connection,
                                            String contentType,
                                            byte[] data) {
    try {
      connection.setRequestMethod("POST");
    } catch (ProtocolException e) {
      LOG.log(Level.SEVERE, "Failed to set post request: ", e);
      return false;
    }

    if (data.length > 0) {
      connection.setRequestProperty(CONTENT_TYPE, contentType);
      connection.setRequestProperty(CONTENT_LENGTH, Integer.toString(data.length));

      connection.setUseCaches(false);
      connection.setDoOutput(true);

      OutputStream os = null;
      try {
        os = connection.getOutputStream();
        os.write(data);
        os.flush();
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to send request: ", e);
        return false;
      } finally {
        try {
          if (os != null) {
            os.close();
          }
        } catch (IOException e) {
          LOG.log(Level.SEVERE, "Failed to close OutputStream: ", e);
          return false;
        }
      }
    }
    return true;
  }

  public static boolean sendHttpGetRequest(HttpURLConnection connection) {
    try {
      connection.setRequestMethod("GET");
      connection.setDoOutput(true);
    } catch (ProtocolException e) {
      LOG.log(Level.SEVERE, "Failed to send http get request: " + connection);
      return false;
    }

    return true;
  }

  public static boolean sendHttpDeleteRequest(HttpURLConnection connection) {
    try {
      connection.setRequestMethod("DELETE");
    } catch (ProtocolException e) {
      LOG.log(Level.SEVERE, "Failed to send http delete request: " + connection);
      return false;
    }

    return true;
  }

  /**
   * Read http response from a given http connection
   *
   * @param connection the connection to read response
   * @return the byte[] in response body, or new byte[0] if failed to read
   */
  public static byte[] readHttpResponse(HttpURLConnection connection) {
    byte[] res;

    try {
      if (connection.getResponseCode() != HttpURLConnection.HTTP_OK) {
        LOG.log(Level.WARNING, "Http Response not OK: " + connection.getResponseCode());
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to get response code", e);
      return new byte[0];
    }

    int responseLength = connection.getContentLength();
    if (responseLength <= 0) {
      LOG.log(Level.SEVERE, "Response length abnormal: " + responseLength);
      return new byte[0];
    }

    try {
      res = new byte[responseLength];

      InputStream is = connection.getInputStream();

      int off = 0;
      int bRead = 0;
      while (off != (responseLength - 1)
          && (bRead = is.read(res, off, responseLength - off)) != -1) {
        off += bRead;
      }

      return res;
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to read response: ", e);
      return new byte[0];
    } finally {
      try {
        connection.getInputStream().close();
      } catch (IOException e) {
        LOG.log(Level.SEVERE, "Failed to close InputStream: ", e);
        return new byte[0];
      }
    }
  }

  public static HttpURLConnection getHttpConnection(String endpoint) {
    try {
      return getHttpConnection(new URL(endpoint), null);
    } catch (MalformedURLException e) {
      LOG.log(Level.SEVERE, "Invalid URL received: " + endpoint, e);
      return null;
    }
  }

  public static HttpURLConnection getProxiedHttpConnectionIfNeeded(URL endpoint,
                                                                   TunnelConfig tunnelConfig) {
    int endpointPort = endpoint.getPort() > 0 ? endpoint.getPort() : endpoint.getDefaultPort();

    if (tunnelConfig != null && tunnelConfig.isTunnelNeeded()) {
      InetSocketAddress socketEndpoint = new InetSocketAddress(endpoint.getHost(), endpointPort);

      Pair<InetSocketAddress, Process> tunnelInfo =
          establishSSHTunnelIfNeeded(socketEndpoint, tunnelConfig, TunnelType.SOCKS_PROXY);
      InetSocketAddress proxyEndpoint = tunnelInfo.first;

      if (socketEndpoint != proxyEndpoint) {
        Proxy proxy = new Proxy(Proxy.Type.SOCKS,
            new InetSocketAddress(proxyEndpoint.getHostName(), proxyEndpoint.getPort()));
        LOG.fine(String.format("setting up proxy. endpoint=%s proxy=%s", socketEndpoint, proxy));
        return getHttpConnection(endpoint, proxy);
      }
    }
    return getHttpConnection(endpoint, null);
  }

  private static HttpURLConnection getHttpConnection(URL endpoint, Proxy proxy) {
    // construct the http url connection
    try {
      if (proxy != null) {
        return (HttpURLConnection) endpoint.openConnection(proxy);
      } else {
        return (HttpURLConnection) endpoint.openConnection();
      }
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Failed to connect to http endpoint " + endpoint, e);
      return null;
    }
  }

  public static boolean checkHttpResponseCode(HttpURLConnection connection, int expectedCode) {
    try {
      return connection.getResponseCode() == expectedCode;
    } catch (IOException ex) {
      LOG.log(Level.SEVERE, "Failed to get response code");
      return false;
    }
  }

  public static String getHostName() {
    String hostName;
    try {
      hostName = InetAddress.getLocalHost().getHostName();
    } catch (UnknownHostException e) {
      LOG.log(Level.SEVERE, "Unable to get local host name", e);
      hostName = "localhost";
    }

    return hostName;
  }

  /**
   * Tests if a network location is reachable. This is best effort and may give false
   * not reachable.
   *
   * @param endpoint the endpoint to connect to
   * @param timeout Open connection will wait for this timeout.
   * @param retryCount In case of connection timeout try retryCount times.
   * @param retryInterval the interval to retryCount
   * @return true if the network location is reachable
   */
  public static boolean isLocationReachable(
      InetSocketAddress endpoint,
      Duration timeout,
      int retryCount,
      Duration retryInterval) {
    int retryLeft = retryCount;
    while (retryLeft > 0) {
      try (Socket s = new Socket()) {
        s.connect(endpoint, (int) timeout.toMillis());
        return true;
      } catch (IOException e) {
      } finally {
        SysUtils.sleep(retryInterval);
        retryLeft--;
      }
    }
    LOG.log(Level.FINE, "Failed to connect to: {0}", endpoint.toString());
    return false;
  }

  public static Pair<InetSocketAddress, Process> establishSSHTunnelIfNeeded(
      InetSocketAddress endpoint, TunnelConfig tunnelConfig, TunnelType tunnelType) {
    return establishSSHTunnelIfNeeded(endpoint, tunnelConfig.getTunnelHost(), tunnelType,
        tunnelConfig.getTimeout(), tunnelConfig.getRetryCount(),
        tunnelConfig.getRetryInterval(), tunnelConfig.getVerifyCount());
  }

  /**
   * Tests if a network location is reachable. This is best effort and may give false
   * not reachable.
   *
   * @param endpoint the endpoint to connect to
   * @param tunnelHost the host used to tunnel
   * @param tunnelType what type of tunnel should be established
   * @param timeout Open connection will wait for this timeout in ms.
   * @param retryCount In case of connection timeout try retryCount times.
   * @param retryInterval the interval in ms to retryCount
   * @param verifyCount In case of longer tunnel setup, try verify times to wait
   * @return a &lt;new_reachable_endpoint, tunnelProcess&gt; pair.
   * If the endpoint already reachable, then new_reachable_endpoint equals to original endpoint, and
   * tunnelProcess is null.
   * If no way to reach even through ssh tunneling,
   * then both new_reachable_endpoint and tunnelProcess are null.
   */
  private static Pair<InetSocketAddress, Process> establishSSHTunnelIfNeeded(
      InetSocketAddress endpoint,
      String tunnelHost,
      TunnelType tunnelType,
      Duration timeout,
      int retryCount,
      Duration retryInterval,
      int verifyCount) {
    if (NetworkUtils.isLocationReachable(endpoint, timeout, retryCount, retryInterval)) {

      // Already reachable, return original endpoint directly
      return new Pair<InetSocketAddress, Process>(endpoint, null);
    } else {
      // Can not reach directly, trying to do ssh tunnel
      int localFreePort = SysUtils.getFreePort();
      InetSocketAddress newEndpoint = new InetSocketAddress(LOCAL_HOST, localFreePort);

      LOG.log(Level.FINE, "Trying to opening up tunnel to {0} from {1}",
          new Object[]{endpoint.toString(), newEndpoint.toString()});

      // Set up the tunnel process
      final Process tunnelProcess;
      switch (tunnelType) {
        case PORT_FORWARD:
          tunnelProcess = ShellUtils.establishSSHTunnelProcess(
              tunnelHost, localFreePort, endpoint.getHostString(), endpoint.getPort());
          break;
        case SOCKS_PROXY:
          tunnelProcess = ShellUtils.establishSocksProxyProcess(tunnelHost, localFreePort);
          break;
        default:
          throw new IllegalArgumentException("Unrecognized TunnelType passed: " + tunnelType);
      }

      // Tunnel can take time to setup.
      // Verify whether the tunnel process is working fine.
      if (tunnelProcess != null && tunnelProcess.isAlive() && NetworkUtils.isLocationReachable(
          newEndpoint, timeout, verifyCount, retryInterval)) {

        java.lang.Runtime.getRuntime().addShutdownHook(new Thread() {
          @Override
          public void run() {
            tunnelProcess.destroy();
          }
        });

        // Can reach the destination via ssh tunnel
        return new Pair<InetSocketAddress, Process>(newEndpoint, tunnelProcess);
      }
      LOG.log(Level.FINE, "Failed to opening up tunnel to {0} from {1}. Releasing process..",
          new Object[]{endpoint, newEndpoint});
      tunnelProcess.destroy();
    }

    // No way to reach the destination. Return null.
    return new Pair<InetSocketAddress, Process>(null, null);
  }

  /**
   * Convert an endpoint from String (host:port) to InetSocketAddress
   *
   * @param endpoint a String in (host:port) format
   * @return an InetSocketAddress representing the endpoint
   */
  public static InetSocketAddress getInetSocketAddress(String endpoint) {
    String[] array = endpoint.split(":");
    return new InetSocketAddress(array[0], Integer.parseInt(array[1]));
  }
}
