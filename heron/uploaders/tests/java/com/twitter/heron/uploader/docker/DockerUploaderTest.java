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
package com.twitter.heron.uploader.docker;

import java.io.File;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentMatcher;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.twitter.heron.spi.common.Config;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Answers.RETURNS_DEEP_STUBS;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class DockerUploaderTest {

  @Mock(answer = RETURNS_DEEP_STUBS)
  private Dockerfile dockerfile;

  @Mock
  private DockerDaemon dockerDaemon;

  private DockerUploader dockerUploader;

  private Config config = Config.newBuilder()
      .put("heron.topology.package.file", "/test/topology_package.tar")
      .put("heron.topology.name", "TestTopology")
      .put("heron.uploader.docker.base", "ubuntu:trusty")
      .put("heron.config.cluster", "local")
      .put("heron.config.role", "default")
      .put("heron.config.environ", "default")
      .build();

  @Before
  public void init() {
    dockerUploader = new DockerUploader(dockerfile, dockerDaemon);
    dockerUploader.initialize(config);
    when(dockerDaemon.build(any(File.class), anyString())).thenReturn(true);
    when(dockerDaemon.push(anyString())).thenReturn(true);
  }

  @Test
  public void testDockerFileCreation() throws Exception {
    Dockerfile.IDockerfileBuilder builder = mock(Dockerfile.IDockerfileBuilder.class);
    when(dockerfile.newDockerfile(any(File.class))).thenReturn(builder);
    when(builder.FROM(anyString())).thenReturn(builder);
    when(builder.ADD(anyString(), anyString())).thenReturn(builder);
    InOrder inOrder = inOrder(builder);
    dockerUploader.uploadPackage();
    verify(dockerfile).newDockerfile(argThat(new ArgumentMatcher<File>() {
      @Override
      public boolean matches(Object argument) {
        return ((File) argument).getPath().equals("/test");
      }
    }));
    inOrder.verify(builder).FROM(eq("ubuntu:trusty"));
    inOrder.verify(builder).ADD(eq("topology_package.tar"), eq("/home/default/TestTopology"));
    inOrder.verify(builder).write();
  }

  @Test
  public void testTagConstruction() {
    assertTrue("Tag doesn't match regex", dockerUploader.uploadPackage().toString()
        .matches("local/default/default/test-topology:[0-9A-Fa-f-]+"));
  }

  @Test
  public void testTagConstructionWithRegistry() {
    dockerUploader.initialize(Config.newBuilder().putAll(config)
        .put("heron.uploader.docker.registry", "dockerUploader.example.com").build());
    assertTrue("Tag doesn't match regex", dockerUploader.uploadPackage().toString()
        .matches("dockerUploader.example.com/local/default/default/test-topology:[0-9A-Fa-f-]+"));
  }

  @Test
  public void testBuilding() {
    final String tag = dockerUploader.uploadPackage().toString();
    verify(dockerDaemon).build(argThat(new ArgumentMatcher<File>() {
      @Override
      public boolean matches(Object argument) {
        return ((File) argument).getPath().equals("/test");
      }
    }), eq(tag));
  }

  @Test
  public void testUploading() {
    dockerUploader.initialize(Config.newBuilder().putAll(config)
        .put("heron.uploader.docker.push", true).build());
    final String tag = dockerUploader.uploadPackage().toString();
    verify(dockerDaemon).push(eq(tag));
  }

  @Test
  public void testNoUploadingDefault() {
    dockerUploader.uploadPackage();
    verify(dockerDaemon, never()).push(anyString());
  }

  @Test
  public void testNoUploadingIfSet() {
    dockerUploader.initialize(Config.newBuilder().putAll(config)
        .put("heron.uploader.docker.push", false).build());
    dockerUploader.uploadPackage();
    verify(dockerDaemon, never()).push(anyString());
  }

  @Test
  public void failedBuildReturnsNull() {
    when(dockerDaemon.build(any(File.class), anyString())).thenReturn(false);
    assertNull(dockerUploader.uploadPackage());
  }

  @Test
  public void failedPushReturnsNull() {
    dockerUploader.initialize(Config.newBuilder().putAll(config)
        .put("heron.uploader.docker.push", true).build());
    when(dockerDaemon.push(anyString())).thenReturn(false);
    assertNull(dockerUploader.uploadPackage());
  }

  @Test
  public void failedWhenNoBase() {
    dockerUploader.initialize(Config.newBuilder()
        .put("heron.topology.package.file", "/test/topology_package.tar")
        .put("heron.topology.name", "TestTopology").build());
    assertNull(dockerUploader.uploadPackage());
  }

}
