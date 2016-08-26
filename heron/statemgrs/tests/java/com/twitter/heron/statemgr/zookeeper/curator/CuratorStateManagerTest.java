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

package com.twitter.heron.statemgr.zookeeper.curator;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.Message;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.api.ACLBackgroundPathAndBytesable;
import org.apache.curator.framework.api.BackgroundCallback;
import org.apache.curator.framework.api.BackgroundPathable;
import org.apache.curator.framework.api.CreateBuilder;
import org.apache.curator.framework.api.CuratorEvent;
import org.apache.curator.framework.api.DeleteBuilder;
import org.apache.curator.framework.api.ExistsBuilder;
import org.apache.curator.framework.api.GetDataBuilder;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.twitter.heron.common.basics.Pair;
import com.twitter.heron.spi.common.Config;
import com.twitter.heron.spi.common.Keys;
import com.twitter.heron.statemgr.zookeeper.ZkContext;

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
    Config.Builder builder = Config.newBuilder()
        .put(Keys.stateManagerRootPath(), ROOT_ADDR)
        .put(Keys.topologyName(), TOPOLOGY_NAME)
        .put(Keys.stateManagerConnectionString(), CONNECTION_STRING);

    // config is used for testing all the methods exception initialize and close
    config = builder.build();

    // tunneling config is used when testing initialize/close method
    tunnelingConfig = builder
        .put(ZkContext.IS_INITIALIZE_TREE, true)
        .put(ZkContext.IS_TUNNEL_NEEDED, true)
        .put(Keys.schedulerService(), false)
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
    CuratorStateManager spyStateManager = Mockito.spy(new CuratorStateManager());
    CuratorFramework mockClient = Mockito.mock(CuratorFramework.class);

    Mockito.doReturn(mockClient)
        .when(spyStateManager).getCuratorClient();
    Mockito.doReturn(new Pair<String, List<Process>>(TUNNEL_STRING, new ArrayList<Process>()))
        .when(spyStateManager).setupZkTunnel();
    Mockito.doReturn(true)
        .when(mockClient).blockUntilConnected(Mockito.anyInt(), Mockito.any(TimeUnit.class));

    spyStateManager.initialize(tunnelingConfig);

    // Make sure tunneling is setup correctly
    Assert.assertTrue(spyStateManager.getConnectionString().equals(TUNNEL_STRING));

    // Verify curator client is invoked
    Mockito.verify(mockClient).start();
    Mockito.verify(mockClient).blockUntilConnected(Mockito.anyInt(), Mockito.any(TimeUnit.class));
    Mockito.verify(mockClient, Mockito.times(6)).createContainers(Mockito.anyString());

    // Verify initTree is called
    Mockito.verify(spyStateManager).initTree();
  }

  /**
   * Test close method
   * @throws Exception
   */
  @Test
  public void testClose() throws Exception {
    CuratorStateManager spyStateManager = Mockito.spy(new CuratorStateManager());
    CuratorFramework mockClient = Mockito.mock(CuratorFramework.class);

    Process mockProcess1 = Mockito.mock(Process.class);
    Process mockProcess2 = Mockito.mock(Process.class);
    List<Process> tunnelProcesses = new ArrayList<>();
    tunnelProcesses.add(mockProcess1);
    tunnelProcesses.add(mockProcess2);

    Mockito.doReturn(mockClient)
        .when(spyStateManager).getCuratorClient();
    Mockito.doReturn(new Pair<>(TUNNEL_STRING, tunnelProcesses))
        .when(spyStateManager).setupZkTunnel();
    Mockito.doReturn(true)
        .when(mockClient).blockUntilConnected(Mockito.anyInt(), Mockito.any(TimeUnit.class));

    spyStateManager.initialize(tunnelingConfig);
    spyStateManager.close();

    // verify curator and processes are closed correctly
    Mockito.verify(mockClient).close();
    Mockito.verify(mockProcess1).destroy();
    Mockito.verify(mockProcess2).destroy();
  }

  /**
   * Test nodeExists method
   * @throws Exception
   */
  @Test
  public void testExistNode() throws Exception {
    CuratorStateManager spyStateManager = Mockito.spy(new CuratorStateManager());
    CuratorFramework mockClient = Mockito.mock(CuratorFramework.class);
    ExistsBuilder mockExistsBuilder = Mockito.mock(ExistsBuilder.class);

    final String correctPath = "/correct/path";
    final String wrongPath = "/wrong/path";

    Mockito.doReturn(mockClient)
        .when(spyStateManager).getCuratorClient();
    Mockito.doReturn(true)
        .when(mockClient).blockUntilConnected(Mockito.anyInt(), Mockito.any(TimeUnit.class));
    Mockito.doReturn(mockExistsBuilder)
        .when(mockClient).checkExists();
    Mockito.doReturn(new Stat())
        .when(mockExistsBuilder).forPath(correctPath);
    Mockito.doReturn(null)
        .when(mockExistsBuilder).forPath(wrongPath);

    spyStateManager.initialize(config);

    // Verify the result is true when path is correct
    ListenableFuture<Boolean> result1 = spyStateManager.nodeExists(correctPath);
    Mockito.verify(mockExistsBuilder).forPath(correctPath);
    Assert.assertTrue(result1.get());

    // Verify the result is false when path is wrong
    ListenableFuture<Boolean> result2 = spyStateManager.nodeExists(wrongPath);
    Mockito.verify(mockExistsBuilder).forPath(wrongPath);
    Assert.assertFalse(result2.get());
  }

  /**
   * test createNode method
   * @throws Exception
   */
  @Test
  public void testCreateNode() throws Exception {
    CuratorStateManager spyStateManager = Mockito.spy(new CuratorStateManager());
    CuratorFramework mockClient = Mockito.mock(CuratorFramework.class);
    CreateBuilder mockCreateBuilder = Mockito.mock(CreateBuilder.class);
    // Mockito doesn't support mock type-parametrized class, thus suppress the warning
    @SuppressWarnings("rawtypes")
    ACLBackgroundPathAndBytesable mockPath = Mockito.spy(ACLBackgroundPathAndBytesable.class);

    final byte[] data = new byte[10];

    Mockito.doReturn(mockClient)
        .when(spyStateManager).getCuratorClient();
    Mockito.doReturn(true)
        .when(mockClient).blockUntilConnected(Mockito.anyInt(), Mockito.any(TimeUnit.class));
    Mockito.doReturn(mockCreateBuilder)
        .when(mockClient).create();
    Mockito.doReturn(mockPath)
        .when(mockCreateBuilder).withMode(Mockito.any(CreateMode.class));

    spyStateManager.initialize(config);

    // Verify the node is created successfully
    ListenableFuture<Boolean> result = spyStateManager.createNode(PATH, data, false);
    Mockito.verify(mockCreateBuilder).withMode(Mockito.any(CreateMode.class));
    Mockito.verify(mockPath).forPath(PATH, data);
    Assert.assertTrue(result.get());
  }

  /**
   * Test deleteNode method
   * @throws Exception
   */
  @Test
  public void testDeleteNode() throws Exception {
    CuratorStateManager spyStateManager = Mockito.spy(new CuratorStateManager());
    CuratorFramework mockClient = Mockito.mock(CuratorFramework.class);
    DeleteBuilder mockDeleteBuilder = Mockito.mock(DeleteBuilder.class);
    // Mockito doesn't support mock type-parametrized class, thus suppress the warning
    @SuppressWarnings("rawtypes")
    BackgroundPathable mockBackPathable = Mockito.mock(BackgroundPathable.class);

    Mockito.doReturn(mockClient)
        .when(spyStateManager).getCuratorClient();
    Mockito.doReturn(true)
        .when(mockClient).blockUntilConnected(Mockito.anyInt(), Mockito.any(TimeUnit.class));
    Mockito.doReturn(mockDeleteBuilder)
        .when(mockClient).delete();
    Mockito.doReturn(mockBackPathable)
        .when(mockDeleteBuilder).withVersion(-1);

    spyStateManager.initialize(config);

    ListenableFuture<Boolean> result = spyStateManager.deleteExecutionState(PATH);

    // Verify the node is deleted correctly
    Mockito.verify(mockDeleteBuilder).withVersion(-1);
    Assert.assertTrue(result.get());
  }

  /**
   * Test getNodeData method
   * @throws Exception
   */
  @Test
  public void testGetNodeData() throws Exception {
    CuratorStateManager spyStateManager = Mockito.spy(new CuratorStateManager());
    final CuratorFramework mockClient = Mockito.mock(CuratorFramework.class);
    GetDataBuilder mockGetBuilder = Mockito.mock(GetDataBuilder.class);
    // Mockito doesn't support mock type-parametrized class, thus suppress the warning
    @SuppressWarnings("rawtypes")
    BackgroundPathable mockBackPathable = Mockito.mock(BackgroundPathable.class);
    final CuratorEvent mockEvent = Mockito.mock(CuratorEvent.class);
    Message.Builder mockBuilder = Mockito.mock(Message.Builder.class);
    Message mockMessage = Mockito.mock(Message.class);

    final byte[] data = "wy_1989".getBytes();

    Mockito.doReturn(mockMessage)
        .when(mockBuilder).build();
    Mockito.doReturn(data)
        .when(mockEvent).getData();
    Mockito.doReturn(PATH)
        .when(mockEvent).getPath();
    Mockito.doReturn(mockClient)
        .when(spyStateManager).getCuratorClient();
    Mockito.doReturn(true)
        .when(mockClient).blockUntilConnected(Mockito.anyInt(), Mockito.any(TimeUnit.class));
    Mockito.doReturn(mockGetBuilder)
        .when(mockClient).getData();
    Mockito.doReturn(mockBackPathable)
        .when(mockGetBuilder).usingWatcher(Mockito.any(Watcher.class));
    Mockito.doAnswer(new Answer<Object>() {
      @Override
      public Object answer(InvocationOnMock invocationOnMock) throws Throwable {
        Object[] objests = invocationOnMock.getArguments();
        // the first object is the BackgroundCallback
        ((BackgroundCallback) objests[0]).processResult(mockClient, mockEvent);
        return null;
      }
    }).when(mockBackPathable).inBackground(Mockito.any(BackgroundCallback.class));

    spyStateManager.initialize(config);

    // Verify the data on node is fetched correctly
    ListenableFuture<Message> result = spyStateManager.getNodeData(null, PATH, mockBuilder);
    Assert.assertTrue(result.get().equals(mockMessage));
  }

  /**
   * Test deleteSchedulerLocation method
   * @throws Exception
   */
  @Test
  public void testDeleteSchedulerLocation() throws Exception {
    CuratorStateManager spyStateManager = Mockito.spy(new CuratorStateManager());
    CuratorFramework mockClient = Mockito.mock(CuratorFramework.class);

    Mockito.doReturn(mockClient)
        .when(spyStateManager).getCuratorClient();
    Mockito.doReturn(true)
        .when(mockClient).blockUntilConnected(Mockito.anyInt(), Mockito.any(TimeUnit.class));

    spyStateManager.initialize(config);

    final SettableFuture<Boolean> fakeResult = SettableFuture.create();
    fakeResult.set(false);
    Mockito.doReturn(fakeResult)
        .when(spyStateManager).deleteNode(Mockito.anyString());

    ListenableFuture<Boolean> result = spyStateManager.deleteSchedulerLocation(TOPOLOGY_NAME);
    Assert.assertTrue(result.get());
  }
}
