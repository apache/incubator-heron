package com.twitter.heron.uploader.localfs;

import java.io.File;
import java.util.HashMap;
import java.util.Properties;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Context;
import com.twitter.heron.spi.common.Misc;
import com.twitter.heron.spi.utils.TopologyTests;

import junit.framework.Assert;

public class LocalFileSystemConfigTest {

  private static final String topologyPackageFile = "/tmp/something.tar.gz";

  private Config getDefaultConfig() {
    Config config = Config.newBuilder()
        .put(Keys.get("CLUSTER"), "cluster")
        .put(Keys.get("ROLE"), "role")
        .put(Keys.get("TOPOLOGY_NAME"), "topology")
        .put(Keys.get("TOPOLOGY_PACKAGE_TYPE"), "tar")
        .put(Keys.get("TOPOLOGY_PACKAGE_FILE"), "/tmp/something.tar.gz")
        .put(LocalFileSystemKeys.get("FILE_SYSTEM_DIRECTORY"), 
             LocalFileSystemDefaults.get("FILE_SYSTEM_DIRECTORY"))
        .build();
    return config;
  }

  @Test
  public void testDefaultConfig() throws Exception {
    Config config = Config.expand(getDefaultConfig());

    Assert.assertEquals(
        LocalFileSystemContext.fileSystemDirectory(config),
        Misc.substitute(config, LocalFileSystemDefaults.get("FILE_SYSTEM_DIRECTORY"))
    );
  }

  @Test
  public void testOverrideConfig() throws Exception {
    String overrideDirectory = "/users/twitter";

    Config config = Config.expand(
        Config.newBuilder()
            .putAll(getDefaultConfig())
            .put(LocalFileSystemKeys.get("FILE_SYSTEM_DIRECTORY"), overrideDirectory)
            .build());

    Assert.assertEquals(
        LocalFileSystemContext.fileSystemDirectory(config),
        overrideDirectory
    );
  }

  @Test
  public void testTopologyDirectory() throws Exception {
    Config config = Config.expand(getDefaultConfig());
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    uploader.initialize(config);

    String destDirectory = Paths.get(LocalFileSystemContext.fileSystemDirectory(config), 
        Context.cluster(config), Context.role(config), Context.topologyName(config)).toString();

    Assert.assertEquals(
        uploader.getTopologyDirectory(),
        Misc.substitute(config, LocalFileSystemDefaults.get("FILE_SYSTEM_DIRECTORY"))
    );
  }

  @Test
  public void testTopologyFile() throws Exception {
    Config config = Config.expand(getDefaultConfig());
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    uploader.initialize(config);

    String destFile = Paths.get(LocalFileSystemContext.fileSystemDirectory(config), 
        new File(topologyPackageFile).getName()).toString();

    Assert.assertEquals(
        uploader.getTopologyFile(),
        destFile
    );
  }

  @Test
  public void testOverrideTopologyDirectory() throws Exception {
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    String overrideDirectory = "/users/twitter";

    Config config = Config.expand(
        Config.newBuilder()
            .putAll(getDefaultConfig())
            .put(LocalFileSystemKeys.get("FILE_SYSTEM_DIRECTORY"), overrideDirectory)
            .build());

    uploader.initialize(config);

    Assert.assertEquals(
        uploader.getTopologyDirectory(),
        overrideDirectory
    );
  }

  @Test
  public void testOverrideTopologyFile() throws Exception {
    LocalFileSystemUploader uploader = new LocalFileSystemUploader();
    String overrideDirectory = "/users/twitter";
    Config config = Config.expand(
        Config.newBuilder()
            .putAll(getDefaultConfig())
            .put(LocalFileSystemKeys.get("FILE_SYSTEM_DIRECTORY"), overrideDirectory)
            .build());

    uploader.initialize(config);

    String destFile = Paths.get(LocalFileSystemContext.fileSystemDirectory(config), 
        new File(topologyPackageFile).getName()).toString();

    Assert.assertEquals(
        uploader.getTopologyFile(),
        destFile
    );
  }
}
