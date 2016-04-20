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

package com.twitter.heron.spi.common;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ProtocolException;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sun.net.httpserver.HttpExchange;

/**
 * Utilities related to network.
 */
public class HttpUtils {
    public static final String CONTENT_LENGTH = "Content-Length";
    public static final String CONTENT_TYPE = "Content-Type";

    private static final Logger LOG = Logger.getLogger(HttpUtils.class.getName());

    /**
     * Blocks current thread
     *
     * @param time Time
     * @param unit Unit
     */
    public static void await(int time, TimeUnit unit) {
        try {
            Thread.sleep(unit.toMillis(time));
        } catch (InterruptedException e) {
            LOG.log(Level.SEVERE, "Sleep interrupted ", e);
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
            while (off != (contentLength - 1) &&
                    (bRead = is.read(requestBody, off, contentLength - off)) != -1) {
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
    public static boolean sendHttpResponse(boolean isSuccess, HttpExchange exchange, byte[] response) {
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
     * @param data the data to send in post request body
     * @return true if success
     */
    public static boolean sendHttpPostRequest(HttpURLConnection connection,
                                              byte[] data) {
        try {
            connection.setRequestMethod("POST");
        } catch (ProtocolException e) {
            LOG.log(Level.SEVERE, "Failed to set post request: ", e);
            return false;
        }

        connection.setRequestProperty(CONTENT_TYPE, "application/x-www-form-urlencoded");

        connection.setRequestProperty(CONTENT_LENGTH, Integer.toString(data.length));

        connection.setUseCaches(false);
        connection.setDoOutput(true);

        try {
            connection.getOutputStream().write(data);
        } catch (IOException e) {
            LOG.log(Level.SEVERE, "Failed to send request: ", e);
            return false;
        } finally {
            try {
                connection.getOutputStream().close();
            } catch (IOException e) {
                LOG.log(Level.SEVERE, "Failed to close OutputStream: ", e);
                return false;
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
                LOG.log(Level.SEVERE, "Http Response not OK: " + connection.getResponseCode());
                return new byte[0];
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
            while (off != (responseLength - 1) &&
                    (bRead = is.read(res, off, responseLength - off)) != -1) {
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

    public static HttpURLConnection getConnection(String endpoint) throws IOException {
        URL url = new URL(endpoint);
        return (HttpURLConnection) url.openConnection();
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
     * @param timeout Open connection will wait for this timeout in ms.
     * @param retry In case of connection timeout try retry times.
     * @param retryInterval the interval in ms to retry
     * @param host host to connect.
     * @param port port on which to connect.
     * @param verbose print verbose results
     * @return true if the network location is reachable
     */
    public static boolean isLocationReachable(int timeout, int retry, int retryInterval, String host, int port, boolean verbose) {
        int retryLeft = retry;
        while (retryLeft > 0) {
            try (Socket s = new Socket()) {
                s.connect(new InetSocketAddress(host, port), timeout);
                s.close();
                return true;
            } catch (SocketTimeoutException se) {
                if (verbose) {
                    LOG.log(Level.INFO, "Couldn't connect to host. Will use tunnelling " + host + ":" + port);
                }
            } catch (IOException ioe) {
                if (verbose) {
                    LOG.log(Level.INFO, "Connection not available yet: " + host + ":" + port, ioe);
                }
            } finally {
                await(retryInterval, TimeUnit.MILLISECONDS);
                retryLeft--;
            }
        }
        LOG.log(Level.SEVERE, "Failed to connect to: " + host + " : " + port);
        return false;
    }
}
