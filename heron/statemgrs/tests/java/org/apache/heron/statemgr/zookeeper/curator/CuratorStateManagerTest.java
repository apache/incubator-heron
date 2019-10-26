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

package org.apache.heron.statemgr.zookeeper.curator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Message;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLBackgroundPathAndBytesable;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.heron.common.basics.Pair;
import org.apache.heron.spi.common.Config;
import org.apache.heron.spi.common.Key;
import org.apache.heron.spi.utils.NetworkUtils;
import org.apache.heron.statemgr.zookeeper.ZkContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * CuratorStateManager Tester.
 */
public class CuratorStateManagerTest {

  private static final String CONNECTION_STRING = "connectionString";
  private static final String PATH = "/heron/n90/path";
  private static final String ROOT_ADDR = "/";
  private static final String TOPOLOGY_NAME = "topology";
  private static final String TUNNEL_STRING = "tunnelConnectionString";

  private Config tunnelingConfig;
  private Config config;

  @Before
  public void before() throws Exception {
    Config.Builder builder = Config.newBuilder(true)
        .put(Key.STATEMGR_ROOT_PATH, ROOT_ADDR)
        .put(Key.TOPOLOGY_NAME, TOPOLOGY_NAME)
        .put(Key.STATEMGR_CONNECTION_STRING, CONNECTION_STRING);

    // config is used for testing all the methods exception initialize and close
    config = builder.build();

    // tunneling config is used when testing initialize/close method
    tunnelingConfig = builder
        .put(ZkContext.IS_INITIALIZE_TREE, true)
        .put(ZkContext.IS_TUNNEL_NEEDED, true)
        .put(Key.SCHEDULER_IS_SERVICE, false)
        .build();
  }

  @After
  public void after() throws Exception {
  }

  /**
   * Test initialize method
   * @throws Exception
   */
  @Test
  public void testInitialize() throws Exception {
    CuratorStateManager spyStateManager = spy(new CuratorStateManager());
    CuratorFramework mockClient = mock(CuratorFramework.class);

    doReturn(mockClient).when(spyStateManager).getCuratorClient();
    doReturn(new Pair<String, List<Process>>(TUNNEL_STRING, new ArrayList<Process>()))
        .when(spyStateManager).setupZkTunnel(any(NetworkUtils.TunnelConfig.class));
    doReturn(true).when(mockClient).blockUntilConnected(anyInt(), any(TimeUnit.class));

    spyStateManager.initialize(tunnelingConfig);

    // Make sure tunneling is setup correctly
    assertTrue(spyStateManager.getConnectionString().equals(TUNNEL_STRING));

    // Verify curator client is invoked
    verify(mockClient).start();
    verify(mockClient).blockUntilConnected(anyInt(), any(TimeUnit.class));
    verify(mockClient, times(9)).createContainers(anyString());

    // Verify initTree is called
    verify(spyStateManager).initTree();
  }

  /**
   * Test close method
   * @throws Exception
   */
  @Test
  public void testClose() throws Exception {
    CuratorStateManager spyStateManager = spy(new CuratorStateManager());
    CuratorFramework mockClient = mock(CuratorFramework.class);

    Process mockProcess1 = mock(Process.class);
    Process mockProcess2 = mock(Process.class);
    List<Process> tunnelProcesses = new ArrayList<>();
    tunnelProcesses.add(mockProcess1);
    tunnelProcesses.add(mockProcess2);

    doReturn(mockClient).when(spyStateManager).getCuratorClient();
    doReturn(new Pair<>(TUNNEL_STRING, tunnelProcesses))
        .when(spyStateManager).setupZkTunnel(any(NetworkUtils.TunnelConfig.class));
    doReturn(true).when(mockClient).blockUntilConnected(anyInt(), any(TimeUnit.class));

    spyStateManager.initialize(tunnelingConfig);
    spyStateManager.close();

    // verify curator and processes are closed correctly
    verify(mockClient).close();
    verify(mockProcess1).destroy();
    verify(mockProcess2).destroy();
  }

  /**
   * Test nodeExists method
   * @throws Exception
   */
  @Test
  public void testExistNode() throws Exception {
    CuratorStateManager spyStateManager = spy(new CuratorStateManager());
    CuratorFramework mockClient = mock(CuratorFramework.class);
    ExistsBuilder mockExistsBuilder = mock(ExistsBuilder.class);

    final String correctPath = "/correct/path";
    final String wrongPath = "/wrong/path";

    doReturn(mockClient)
        .when(spyStateManager).getCuratorClient();
    doReturn(true)
        .when(mockClient).blockUntilConnected(anyInt(), any(TimeUnit.class));
    doReturn(mockExistsBuilder)
        .when(mockClient).checkExists();
    doReturn(new Stat())
        .when(mockExistsBuilder).forPath(correctPath);
    doReturn(null)
        .when(mockExistsBuilder).forPath(wrongPath);

    spyStateManager.initialize(config);

    // Verify the result is true when path is correct
    ListenableFuture<Boolean> result1 = spyStateManager.nodeExists(correctPath);
    verify(mockExistsBuilder).forPath(correctPath);
    assertTrue(result1.get());

    // Verify the result is false when path is wrong
    ListenableFuture<Boolean> result2 = spyStateManager.nodeExists(wrongPath);
    verify(mockExistsBuilder).forPath(wrongPath);
    assertFalse(result2.get());
  }

  /**
   * test createNode method
   * @throws Exception
   */
  @Test
  public void testCreateNode() throws Exception {
    CuratorStateManager spyStateManager = spy(new CuratorStateManager());
    CuratorFramework mockClient = mock(CuratorFramework.class);
    CreateBuilder mockCreateBuilder = mock(CreateBuilder.class);
    // Mockito doesn't support mock type-parametrized class, thus suppress the warning
    @SuppressWarnings("rawtypes")
    ACLBackgroundPathAndBytesable mockPath = spy(ACLBackgroundPathAndBytesable.class);

    final byte[] data = new byte[10];

    doReturn(mockClient)
        .when(spyStateManager).getCuratorClient();
    doReturn(true)
        .when(mockClient).blockUntilConnected(anyInt(), any(TimeUnit.class));
    doReturn(mockCreateBuilder)
        .when(mockClient).create();
    doReturn(mockPath)
        .when(mockCreateBuilder).withMode(any(CreateMode.class));

    spyStateManager.initialize(config);

    // Verify the node is created successfully
    ListenableFuture<Boolean> result = spyStateManager.createNode(PATH, data, false);
    verify(mockCreateBuilder).withMode(any(CreateMode.class));
    verify(mockPath).forPath(PATH, data);
    assertTrue(result.get());
  }

  /**
   * Test deleteNode method
   * @throws Exception
   */
  @Test
  public void testDeleteNode() throws Exception {
    CuratorStateManager spyStateManager = spy(new CuratorStateManager());
    CuratorFramework mockClient = mock(CuratorFramework.class);
    DeleteBuilder mockDeleteBuilder = mock(DeleteBuilder.class);
    // Mockito doesn't support mock type-parametrized class, thus suppress the warning
    @SuppressWarnings("rawtypes")
    BackgroundPathable mockBackPathable = mock(BackgroundPathable.class);

    doReturn(mockClient)
        .when(spyStateManager).getCuratorClient();
    doReturn(true)
        .when(mockClient).blockUntilConnected(anyInt(), any(TimeUnit.class));
    doReturn(mockDeleteBuilder)
        .when(mockClient).delete();
    doReturn(mockBackPathable)
        .when(mockDeleteBuilder).withVersion(-1);

    spyStateManager.initialize(config);

    ListenableFuture<Boolean> result = spyStateManager.deleteExecutionState(PATH);

    // Verify the node is deleted correctly
    verify(mockDeleteBuilder).withVersion(-1);
    assertTrue(result.get());
  }

  /**
   * Test getNodeData method
   * @throws Exception
   */
  @Test
  public void testGetNodeData() throws Exception {
    CuratorStateManager spyStateManager = spy(new CuratorStateManager());
    final CuratorFramework mockClient = mock(CuratorFramework.class);
    GetDataBuilder mockGetBuilder = mock(GetDataBuilder.class);
    // Mockito doesn't support mock type-parametrized class, thus suppress the warning
    @SuppressWarnings("rawtypes")
    BackgroundPathable mockBackPathable = mock(BackgroundPathable.class);
    final CuratorEvent mockEvent = mock(CuratorEvent.class);
    Message.Builder mockBuilder = mock(Message.Builder.class);
    Message mockMessage = mock(Message.class);

    final byte[] data = "wy_1989".getBytes();

    doReturn(mockMessage)
        .when(mockBuilder).build();
    doReturn(data)
        .when(mockEvent).getData();
    doReturn(PATH)
        .when(mockEvent).getPath();
    doReturn(mockClient)
        .when(spyStateManager).getCuratorClient();
    doReturn(true)
        .when(mockClient).blockUntilConnected(anyInt(), any(TimeUnit.class));
    doReturn(mockGetBuilder)
        .when(mockClient).getData();
    doReturn(mockBackPathable)
        .when(mockGetBuilder).usingWatcher(any(Watcher.class));
    doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        Object[] objests = invocationOnMock.getArguments();
        // the first object is the BackgroundCallback
        ((BackgroundCallback) objests[0]).processResult(mockClient, mockEvent);
        return null;
      }
    }).when(mockBackPathable).inBackground(any(BackgroundCallback.class));

    spyStateManager.initialize(config);

    // Verify the data on node is fetched correctly
    ListenableFuture<Message> result = spyStateManager.getNodeData(null, PATH, mockBuilder);
    assertTrue(result.get().equals(mockMessage));
  }

  /**
   * Test deleteSchedulerLocation method
   * @throws Exception
   */
  @Test
  public void testDeleteSchedulerLocation() throws Exception {
    CuratorStateManager spyStateManager = spy(new CuratorStateManager());
    CuratorFramework mockClient = mock(CuratorFramework.class);

    doReturn(mockClient)
        .when(spyStateManager).getCuratorClient();
    doReturn(true)
        .when(mockClient).blockUntilConnected(anyInt(), any(TimeUnit.class));

    spyStateManager.initialize(config);

    final SettableFuture<Boolean> fakeResult = SettableFuture.create();
    fakeResult.set(false);
    doReturn(fakeResult).when(spyStateManager).deleteNode(anyString(), anyBoolean());

    ListenableFuture<Boolean> result = spyStateManager.deleteSchedulerLocation(TOPOLOGY_NAME);
    assertTrue(result.get());
  }
}
