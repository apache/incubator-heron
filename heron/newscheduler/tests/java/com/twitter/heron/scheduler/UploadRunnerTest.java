package com.twitter.heron.scheduler.service;

import java.util.HashMap;

import org.junit.Test;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;

import com.twitter.heron.spi.scheduler.IConfigLoader;
import com.twitter.heron.spi.uploader.IUploader;
import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.scheduler.util.TopologyUtilityTest;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

public class UploadRunnerTest {
  @Test
  public void testUploadRunner() {
    String topologyPkg = "topology.tar.gz";
    TopologyAPI.Topology topology = TopologyUtilityTest.createTopology(
        "name", new Config(), new HashMap<String, Integer>(), new HashMap<String, Integer>());

    IUploader uploader = mock(IUploader.class);
    when(uploader.uploadPackage(anyString())).thenReturn(true);
    IConfigLoader config = mock(IConfigLoader.class);
    UploadRunner runner = new UploadRunner(uploader, mock(LaunchContext.class), topologyPkg);
    assertTrue(runner.call());
    inOrder(uploader);
    verify(uploader).initialize(any(LaunchContext.class));
    verify(uploader).uploadPackage(eq(topologyPkg));
  }
}
