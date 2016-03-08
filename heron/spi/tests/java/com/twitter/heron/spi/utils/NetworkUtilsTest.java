package com.twitter.heron.spi.utils;

import java.io.IOException;
import java.net.SocketException;
import java.net.ServerSocket;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.twitter.heron.proto.system.Common;

public class NetworkUtilsTest {

  @Test
  public void testFreePort() {
    int numAttempts = 100;
    // Randomized test
    for (int i = 0; i < numAttempts; ++i) {
      int port = NetworkUtils.getFreePort();
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
  public void testGetHeronStatus() {
    Common.Status okStatus = Common.Status.newBuilder().
        setStatus(Common.StatusCode.OK)
        .build();
    Assert.assertEquals(okStatus, NetworkUtils.getHeronStatus(true));

    Common.Status notOKStatus = Common.Status.newBuilder()
        .setStatus(Common.StatusCode.NOTOK)
        .build();
    Assert.assertEquals(notOKStatus, NetworkUtils.getHeronStatus(false));

  }
}
