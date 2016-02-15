package com.twitter.heron.scheduler.local;

import java.util.HashMap;
import java.util.Properties;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.generated.TopologyAPI;
import com.twitter.heron.common.basics.FileUtility;
import com.twitter.heron.spi.common.Constants;
import com.twitter.heron.spi.scheduler.context.LaunchContext;
import com.twitter.heron.scheduler.util.DefaultConfigLoader;
import com.twitter.heron.scheduler.util.TopologyUtilityTest;

import junit.framework.Assert;

/**
 * LocalUploader Tester.
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(FileUtility.class)

public class LocalUploaderTest {
  public static final String working_directory = "working-dir";
  public static final String heron_core_release = "heron-core-release";
  public static final String stateMgrClass = "com.twitter.heron.statemgr.NullStateManager";

  public static DefaultConfigLoader getDefaultConfigLoader() throws Exception {
    DefaultConfigLoader configLoader = DefaultConfigLoader.class.newInstance();
    configLoader.addDefaultProperties();
    configLoader.properties.setProperty(LocalConfig.WORKING_DIRECTORY, working_directory);
    configLoader.properties.setProperty(
        LocalConfig.HERON_CORE_RELEASE_PACKAGE, heron_core_release);
    configLoader.properties.setProperty(Constants.STATE_MANAGER_CLASS, stateMgrClass);
    return configLoader;
  }

  @Before
  public void before() throws Exception {

  }

  @After
  public void after() throws Exception {
  }

  @Test
  public void testLocalUploader() throws Exception {
    String topologyPkg = "someTopologyPkg.tar";
    TopologyAPI.Topology topology = TopologyUtilityTest.createTopology(
        "name", new Config(), new HashMap<String, Integer>(), new HashMap<String, Integer>());

    LocalUploader uploader = Mockito.spy(LocalUploader.class.newInstance());

    PowerMockito.mockStatic(FileUtility.class);
    Mockito.when(FileUtility.isDirectoryExists(Matchers.anyString())).thenReturn(true);
    Mockito.when(FileUtility.copyFile(Matchers.anyString(), Matchers.anyString())).thenReturn(true);
    Mockito.when(FileUtility.getBaseName(topologyPkg)).thenReturn(topologyPkg);

    DefaultConfigLoader configLoader = getDefaultConfigLoader();
    LaunchContext context =
        new LaunchContext(configLoader, topology);
    uploader.initialize(context);

    Assert.assertTrue(uploader.uploadPackage(topologyPkg));

    String expectedTarget = String.format("%s/%s",
        working_directory, topologyPkg);

    PowerMockito.verifyStatic();
    FileUtility.copyFile(topologyPkg, expectedTarget);
  }

  @Test
  public void testLocalUploaderFail() throws Exception {
    String topologyPkg = "someTopologyPkg.tar";
    TopologyAPI.Topology topology = TopologyUtilityTest.createTopology(
        "name", new Config(), new HashMap<String, Integer>(), new HashMap<String, Integer>());

    LocalUploader uploader = Mockito.spy(LocalUploader.class.newInstance());

    PowerMockito.mockStatic(FileUtility.class);
    Mockito.when(FileUtility.isDirectoryExists(Matchers.anyString())).thenReturn(true);
    Mockito.when(FileUtility.copyFile(Matchers.anyString(), Matchers.anyString())).thenReturn(false);
    Mockito.when(FileUtility.getBaseName(topologyPkg)).thenReturn(topologyPkg);

    DefaultConfigLoader configLoader = getDefaultConfigLoader();
    LaunchContext context =
        new LaunchContext(configLoader, topology);
    uploader.initialize(context);

    Assert.assertFalse(uploader.uploadPackage(topologyPkg));

    String expectedTarget = String.format("%s/%s",
        working_directory, topologyPkg);

    PowerMockito.verifyStatic();
    FileUtility.copyFile(topologyPkg, expectedTarget);
  }

  /**
   * Method: undo()
   */
  @Test
  public void testUndo() throws Exception {
    LocalUploader uploader = Mockito.spy(LocalUploader.class.newInstance());

    PowerMockito.mockStatic(FileUtility.class);
    Mockito.when(FileUtility.deleteFile(Matchers.anyString())).thenReturn(true);

    uploader.undo();

    PowerMockito.verifyStatic();
    FileUtility.deleteFile(Matchers.anyString());
  }
}
