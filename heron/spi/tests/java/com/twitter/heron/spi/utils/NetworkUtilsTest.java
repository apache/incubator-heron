package com.twitter.heron.spi.utils;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.twitter.heron.proto.system.Common;

public class NetworkUtilsTest {

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
