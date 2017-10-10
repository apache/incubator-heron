//  Copyright 2017 Twitter. All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package com.twitter.heron.statefulstorage.dlog;

import java.io.InputStream;
import java.net.URI;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.google.common.collect.Lists;

import org.apache.distributedlog.AppendOnlyStreamWriter;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import com.twitter.heron.proto.ckptmgr.CheckpointManager;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.spi.statefulstorage.Checkpoint;
import com.twitter.heron.statefulstorage.StatefulStorageTestContext;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(PowerMockRunner.class)
@PrepareForTest({Namespace.class, CheckpointManager.InstanceStateCheckpoint.class})
public class DlogStorageTest {

  private static final String ROOT_URI = "distributedlog://127.0.0.1/heron/statefulstorage";

  private PhysicalPlans.Instance instance;
  private CheckpointManager.InstanceStateCheckpoint instanceStateCheckpoint;

  private DlogStorage dlogStorage;
  private NamespaceBuilder mockNsBuilder;
  private Namespace mockNamespace;

  @Before
  public void before() throws Exception {
    Map<String, Object> config = new HashMap<>();
    config.put(DlogStorage.NS_URI_KEY, ROOT_URI);

    mockNamespace = mock(Namespace.class);
    mockNsBuilder = mock(NamespaceBuilder.class);
    when(mockNsBuilder.clientId(anyString())).thenReturn(mockNsBuilder);
    when(mockNsBuilder.conf(any(DistributedLogConfiguration.class))).thenReturn(mockNsBuilder);
    when(mockNsBuilder.uri(any(URI.class))).thenReturn(mockNsBuilder);
    when(mockNsBuilder.build()).thenReturn(mockNamespace);

    dlogStorage = new DlogStorage(() -> mockNsBuilder);
    dlogStorage.init(config);
    dlogStorage = spy(dlogStorage);

    instance = StatefulStorageTestContext.getInstance();
    instanceStateCheckpoint = StatefulStorageTestContext.getInstanceStateCheckpoint();
  }

  @After
  public void after() throws Exception {
    dlogStorage.close();
  }

  @Test
  public void testStore() throws Exception {
    PowerMockito.mockStatic(CheckpointManager.InstanceStateCheckpoint.class);
    CheckpointManager.InstanceStateCheckpoint mockCheckpointState =
        mock(CheckpointManager.InstanceStateCheckpoint.class);

    Checkpoint checkpoint =
        new Checkpoint(StatefulStorageTestContext.TOPOLOGY_NAME, instance, mockCheckpointState);

    DistributedLogManager mockDLM = mock(DistributedLogManager.class);
    when(mockNamespace.openLog(anyString())).thenReturn(mockDLM);
    AppendOnlyStreamWriter mockWriter = mock(AppendOnlyStreamWriter.class);
    when(mockDLM.getAppendOnlyStreamWriter()).thenReturn(mockWriter);

    dlogStorage.store(checkpoint);

    verify(mockWriter).markEndOfStream();
    verify(mockWriter).close();
  }

  @Test
  public void testRestore() throws Exception {
    Checkpoint restoreCheckpoint =
        new Checkpoint(StatefulStorageTestContext.TOPOLOGY_NAME, instance, instanceStateCheckpoint);

    InputStream mockInputStream = mock(InputStream.class);
    doReturn(mockInputStream).when(dlogStorage).openInputStream(anyString());

    PowerMockito.spy(CheckpointManager.InstanceStateCheckpoint.class);
    PowerMockito.doReturn(instanceStateCheckpoint)
        .when(CheckpointManager.InstanceStateCheckpoint.class,
            "parseFrom", mockInputStream);

    dlogStorage.restore(
        StatefulStorageTestContext.TOPOLOGY_NAME,
        StatefulStorageTestContext.CHECKPOINT_ID,
        instance);
    assertEquals(restoreCheckpoint.getCheckpoint(), instanceStateCheckpoint);
  }

  @Test
  public void testDiposeAll() throws Exception {
    Namespace mockTopoloyNs = mock(Namespace.class);
    Namespace mockCheckpoint1 = mock(Namespace.class);
    Namespace mockCheckpoint2 = mock(Namespace.class);

    String checkpoint1 = "checkpoint1";
    String checkpoint2 = "checkpoint2";

    List<String> checkpoints = Lists.newArrayList(checkpoint1, checkpoint2);
    List<String> chkp1Tasks = Lists.newArrayList(
        "component1_task1",
        "component1_task2");
    List<String> chkp2Tasks = Lists.newArrayList(
        "component2_task1",
        "component2_task2");

    doReturn(mockTopoloyNs).when(dlogStorage)
        .initializeNamespace(eq(
            URI.create(ROOT_URI + "/" + StatefulStorageTestContext.TOPOLOGY_NAME)));
    doReturn(mockCheckpoint1).when(dlogStorage)
        .initializeNamespace(eq(
            URI.create(ROOT_URI + "/" + StatefulStorageTestContext.TOPOLOGY_NAME
                + "/checkpoint1")));
    doReturn(mockCheckpoint2).when(dlogStorage)
        .initializeNamespace(eq(
            URI.create(ROOT_URI + "/" + StatefulStorageTestContext.TOPOLOGY_NAME
                + "/checkpoint2")));

    when(mockTopoloyNs.getLogs()).thenReturn(checkpoints.iterator());
    when(mockCheckpoint1.getLogs()).thenReturn(chkp1Tasks.iterator());
    when(mockCheckpoint2.getLogs()).thenReturn(chkp2Tasks.iterator());

    dlogStorage.dispose(
        StatefulStorageTestContext.TOPOLOGY_NAME,
        "checkpoint0",
        true);

    verify(mockCheckpoint1, times(1)).deleteLog(eq("component1_task1"));
    verify(mockCheckpoint1, times(1)).deleteLog(eq("component1_task2"));
    verify(mockCheckpoint2, times(1)).deleteLog(eq("component2_task1"));
    verify(mockCheckpoint2, times(1)).deleteLog(eq("component2_task2"));
  }

  @Test
  public void testDiposeNone() throws Exception {
    Namespace mockTopoloyNs = mock(Namespace.class);
    Namespace mockCheckpoint1 = mock(Namespace.class);
    Namespace mockCheckpoint2 = mock(Namespace.class);

    String checkpoint1 = "checkpoint1";
    String checkpoint2 = "checkpoint2";

    List<String> checkpoints = Lists.newArrayList(checkpoint1, checkpoint2);
    List<String> chkp1Tasks = Lists.newArrayList(
        "component1_task1",
        "component1_task2");
    List<String> chkp2Tasks = Lists.newArrayList(
        "component2_task1",
        "component2_task2");

    doReturn(mockTopoloyNs).when(dlogStorage)
        .initializeNamespace(eq(
            URI.create(ROOT_URI + "/" + StatefulStorageTestContext.TOPOLOGY_NAME)));
    doReturn(mockCheckpoint1).when(dlogStorage)
        .initializeNamespace(eq(
            URI.create(ROOT_URI + "/" + StatefulStorageTestContext.TOPOLOGY_NAME
                + "/checkpoint1")));
    doReturn(mockCheckpoint2).when(dlogStorage)
        .initializeNamespace(eq(
            URI.create(ROOT_URI + "/" + StatefulStorageTestContext.TOPOLOGY_NAME
                + "/checkpoint2")));

    when(mockTopoloyNs.getLogs()).thenReturn(checkpoints.iterator());
    when(mockCheckpoint1.getLogs()).thenReturn(chkp1Tasks.iterator());
    when(mockCheckpoint2.getLogs()).thenReturn(chkp2Tasks.iterator());

    dlogStorage.dispose(
        StatefulStorageTestContext.TOPOLOGY_NAME,
        "checkpoint0",
        false);

    verify(mockCheckpoint1, times(0)).deleteLog(eq("component1_task1"));
    verify(mockCheckpoint1, times(0)).deleteLog(eq("component1_task2"));
    verify(mockCheckpoint2, times(0)).deleteLog(eq("component2_task1"));
    verify(mockCheckpoint2, times(0)).deleteLog(eq("component2_task2"));
  }

  @Test
  public void testDiposePartial() throws Exception {
    Namespace mockTopoloyNs = mock(Namespace.class);
    Namespace mockCheckpoint1 = mock(Namespace.class);
    Namespace mockCheckpoint2 = mock(Namespace.class);

    String checkpoint1 = "checkpoint1";
    String checkpoint2 = "checkpoint2";

    List<String> checkpoints = Lists.newArrayList(checkpoint1, checkpoint2);
    List<String> chkp1Tasks = Lists.newArrayList(
        "component1_task1",
        "component1_task2");
    List<String> chkp2Tasks = Lists.newArrayList(
        "component2_task1",
        "component2_task2");

    doReturn(mockTopoloyNs).when(dlogStorage)
        .initializeNamespace(eq(
            URI.create(ROOT_URI + "/" + StatefulStorageTestContext.TOPOLOGY_NAME)));
    doReturn(mockCheckpoint1).when(dlogStorage)
        .initializeNamespace(eq(
            URI.create(ROOT_URI + "/" + StatefulStorageTestContext.TOPOLOGY_NAME
                + "/checkpoint1")));
    doReturn(mockCheckpoint2).when(dlogStorage)
        .initializeNamespace(eq(
            URI.create(ROOT_URI + "/" + StatefulStorageTestContext.TOPOLOGY_NAME
                + "/checkpoint2")));

    when(mockTopoloyNs.getLogs()).thenReturn(checkpoints.iterator());
    when(mockCheckpoint1.getLogs()).thenReturn(chkp1Tasks.iterator());
    when(mockCheckpoint2.getLogs()).thenReturn(chkp2Tasks.iterator());

    dlogStorage.dispose(
        StatefulStorageTestContext.TOPOLOGY_NAME,
        "checkpoint2",
        false);

    verify(mockCheckpoint1, times(1)).deleteLog(eq("component1_task1"));
    verify(mockCheckpoint1, times(1)).deleteLog(eq("component1_task2"));
    verify(mockCheckpoint2, times(0)).deleteLog(eq("component2_task1"));
    verify(mockCheckpoint2, times(0)).deleteLog(eq("component2_task2"));
  }

}
