package com.twitter.heron.scheduler.twitter;

import java.util.HashMap;
import java.util.Properties;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.scheduler.context.LaunchContext;

import com.twitter.heron.scheduler.util.DefaultConfigLoader;
import com.twitter.heron.scheduler.util.TopologyUtilityTest;

public class PackerUploaderTest {
  public static final String dc = "cluster";
  public static final String role = "me";
  public static final String pkgName = "pkg";

  public static DefaultConfigLoader getRequiredConfig() throws Exception {
    DefaultConfigLoader configLoader = DefaultConfigLoader.class.newInstance();
    configLoader.addDefaultProperties();
    configLoader.properties.setProperty(Constants.DC, dc);
    configLoader.properties.setProperty(Constants.ROLE, role);
    configLoader.properties.setProperty(Constants.HERON_RELEASE_PACKAGE_NAME, pkgName);
    return configLoader;
  }

  @Test
  public void testPackerUploader() throws Exception {
    String topologyPkg = "someTopologyPkg.tar";
    TopologyAPI.Topology topology = TopologyUtilityTest.createTopology(
        "name", new com.twitter.heron.api.Config(), new HashMap<String, Integer>(), new HashMap<String, Integer>());
    PackerUploader uploader = Mockito.spy(PackerUploader.class.newInstance());
    Mockito.doReturn(0).when(uploader).runProcess(Matchers.anyString(), Matchers.any(StringBuilder.class));
    Mockito.doReturn("").when(uploader).getTopologyURI(Matchers.anyString());
    DefaultConfigLoader config = getRequiredConfig();
    LaunchContext context =
        new LaunchContext(config, topology);
    uploader.initialize(context);
    Assert.assertTrue(uploader.uploadPackage(topologyPkg));
    String expectedPackerAddCmd = String.format("packer add_version --cluster %s %s %s %s --json",
        dc, role, uploader.getTopologyPackageName(), topologyPkg);
    Mockito.verify(uploader).runProcess(Matchers.eq(expectedPackerAddCmd), Matchers.any(StringBuilder.class));
    String expectedPackerSetLiveCmd = String.format("packer set_live --cluster %s %s %s latest",
        dc, role, uploader.getTopologyPackageName());
    Mockito.verify(uploader).runProcess(Matchers.eq(expectedPackerSetLiveCmd), Matchers.any(StringBuilder.class));
  }

  @Test
  public void testPackerUploaderFail() throws Exception {
    String topologyPkg = "someTopologyPkg.tar";
    TopologyAPI.Topology topology = TopologyUtilityTest.createTopology(
        "name", new com.twitter.heron.api.Config(), new HashMap<String, Integer>(), new HashMap<String, Integer>());
    PackerUploader uploader = Mockito.spy(PackerUploader.class.newInstance());

    // packer add_version will return fail.
    Mockito.doReturn(0).when(uploader).runProcess(Matchers.anyString(), Matchers.any(StringBuilder.class));
    Mockito.doReturn(1).when(uploader).runProcess(Matchers.startsWith("packer add_version"), Matchers.any(StringBuilder.class));
    Mockito.doReturn("").when(uploader).getTopologyURI(Matchers.anyString());

    DefaultConfigLoader config = getRequiredConfig();
    LaunchContext context =
        new LaunchContext(config, topology);
    uploader.initialize(context);
    Assert.assertFalse(uploader.uploadPackage(topologyPkg));
    String expectedPackerAddCmd = String.format("packer add_version --cluster %s %s %s %s --json",
        dc, role, uploader.getTopologyPackageName(), topologyPkg);
    Mockito.verify(uploader).runProcess(Matchers.eq(expectedPackerAddCmd), Matchers.any(StringBuilder.class));
    String expectedPackerSetLiveCmd = String.format("packer set_live --cluster %s %s %s latest",
        dc, role, uploader.getTopologyPackageName());
    Mockito.verify(uploader, Mockito.never()).runProcess(Matchers.eq(expectedPackerSetLiveCmd), Matchers.any(StringBuilder.class));
  }
}
