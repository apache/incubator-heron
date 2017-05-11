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

package com.twitter.heron.ckptmgr;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.network.HeronClient;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.network.StatusCode;
import com.twitter.heron.proto.ckptmgr.CheckpointManager;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.spi.statefulstorage.Checkpoint;
import com.twitter.heron.spi.statefulstorage.IStatefulStorage;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class CheckpointManagerServerTest {

  private static final String TOPOLOGY_NAME = "topology_name";
  private static final String TOPOLOGY_ID = "topology_id";
  private static final String CHECKPOINT_MANAGER_ID = "ckptmgr_id";
  private static final String INSTANCE_ID = "instance_id";
  private static final String STMGR_ID = "stmgr_id";
  private static final int TASK_ID = 1;
  private static final int COMPONENT_INDEX = 1;
  private static final String CHECKPOINT_ID = "checkpoint_id";
  private static final String COMPONENT_NAME = "component_name";
  private static final byte[] BYTES = "checkpoint manager server test bytes".getBytes();

  private static final String SERVER_HOST = "127.0.0.1";
  private static int serverPort;

  private static CheckpointManager.SaveInstanceStateRequest saveInstanceStateRequest;
  private static CheckpointManager.GetInstanceStateRequest getInstanceStateRequest;
  private static CheckpointManager.CleanStatefulCheckpointRequest cleanStatefulCheckpointRequest;
  private static CheckpointManager.RegisterStMgrRequest registerStmgrRequest;
  private static CheckpointManager.RegisterTMasterRequest registerTMasterRequest;

  private static PhysicalPlans.Instance instance;

  private CheckpointManagerServer checkpointManagerServer;
  private SimpleCheckpointManagerClient simpleCheckpointManagerClient;

  private NIOLooper serverLooper;
  private NIOLooper clientLooper;
  private ExecutorService threadPools;
  private CountDownLatch finishedSignal;

  private IStatefulStorage backendStorage;

  @BeforeClass
  public static void setup() throws Exception {
    PhysicalPlans.InstanceInfo info = PhysicalPlans.InstanceInfo.newBuilder()
        .setTaskId(TASK_ID)
        .setComponentIndex(COMPONENT_INDEX)
        .setComponentName(COMPONENT_NAME)
        .build();

    instance = PhysicalPlans.Instance.newBuilder()
        .setInstanceId(INSTANCE_ID)
        .setStmgrId(STMGR_ID)
        .setInfo(info)
        .build();

    CheckpointManager.InstanceStateCheckpoint checkpoint =
        CheckpointManager.InstanceStateCheckpoint.newBuilder()
            .setCheckpointId(CHECKPOINT_ID)
            .setState(ByteString.copyFrom(BYTES))
            .build();

    saveInstanceStateRequest = CheckpointManager.SaveInstanceStateRequest.newBuilder()
        .setInstance(instance)
        .setCheckpoint(checkpoint)
        .build();

    getInstanceStateRequest = CheckpointManager.GetInstanceStateRequest.newBuilder()
        .setInstance(instance)
        .setCheckpointId(CHECKPOINT_ID)
        .build();

    cleanStatefulCheckpointRequest = CheckpointManager.CleanStatefulCheckpointRequest.newBuilder()
        .setCleanAllCheckpoints(true)
        .setOldestCheckpointPreserved(CHECKPOINT_ID)
        .build();

    registerStmgrRequest = CheckpointManager.RegisterStMgrRequest.newBuilder()
        .setTopologyId(TOPOLOGY_ID)
        .setStmgrId(STMGR_ID)
        .setTopologyName(TOPOLOGY_NAME)
        .build();

    registerTMasterRequest = CheckpointManager.RegisterTMasterRequest.newBuilder()
        .setTopologyId(TOPOLOGY_ID)
        .setTopologyName(TOPOLOGY_NAME)
        .build();
  }

  @Before
  public void before() throws Exception {
    finishedSignal = new CountDownLatch(1);

    serverLooper = new NIOLooper();
    clientLooper = new NIOLooper();
    threadPools = Executors.newFixedThreadPool(2);

    backendStorage = mock(IStatefulStorage.class);

    HeronSocketOptions serverSocketOptions =
        new HeronSocketOptions(100 * 1024 * 1024, 100,
            100 * 1024 * 1024, 100,
            5 * 1024 * 1024,
            5 * 1024 * 1024);

    serverPort = SysUtils.getFreePort();
    checkpointManagerServer = new CheckpointManagerServer(
        TOPOLOGY_NAME, TOPOLOGY_ID,
        CHECKPOINT_MANAGER_ID, backendStorage,
        serverLooper, SERVER_HOST, serverPort, serverSocketOptions);

    runServer();
  }

  @After
  public void after() {
    threadPools.shutdownNow();

    checkpointManagerServer.stop();
    checkpointManagerServer = null;

    if (simpleCheckpointManagerClient != null) {
      simpleCheckpointManagerClient.stop();

      simpleCheckpointManagerClient.getNIOLooper().exitLoop();
    }

    serverLooper.exitLoop();
    serverLooper = null;

    threadPools = null;
  }

  @Test
  public void testSaveInstanceState() throws Exception {
    prepareClient(SimpleCheckpointManagerClient.RequestType.SAVE_INSTANCE_STATE);

    runClient(simpleCheckpointManagerClient);
    // let the client start and send request
    finishedSignal.await(2, TimeUnit.SECONDS);

    verify(backendStorage).store(any(Checkpoint.class));
    assertEquals(StatusCode.OK, simpleCheckpointManagerClient.getResponseStatus());
    Message response = simpleCheckpointManagerClient.getResponse();
    assertTrue(response instanceof CheckpointManager.SaveInstanceStateResponse);
    assertEquals(CHECKPOINT_ID,
        ((CheckpointManager.SaveInstanceStateResponse) response).getCheckpointId());
    assertEquals(instance,
        ((CheckpointManager.SaveInstanceStateResponse) response).getInstance());
  }

  @Test
  public void testGetInstanceState() throws Exception {
    doAnswer(new Answer<Checkpoint>() {
      @Override
      public Checkpoint answer(InvocationOnMock invocationOnMock) throws Throwable {
        Checkpoint checkpoint = (Checkpoint) invocationOnMock.getArguments()[0];
        checkpoint.setCheckpoint(saveInstanceStateRequest);
        return checkpoint;
      }
    }).when(backendStorage).restore(any(Checkpoint.class));

    prepareClient(SimpleCheckpointManagerClient.RequestType.GET_INSTANCE_STATE);

    runClient(simpleCheckpointManagerClient);
    finishedSignal.await(2, TimeUnit.SECONDS);

    verify(backendStorage).restore(any(Checkpoint.class));
    assertEquals(StatusCode.OK, simpleCheckpointManagerClient.getResponseStatus());

    Message response = simpleCheckpointManagerClient.getResponse();
    assertTrue(response instanceof CheckpointManager.GetInstanceStateResponse);
    assertEquals(saveInstanceStateRequest.getCheckpoint(),
        ((CheckpointManager.GetInstanceStateResponse) response).getCheckpoint());
  }

  @Test
  public void testCleanStatefulCheckpoint() throws Exception {
    prepareClient(SimpleCheckpointManagerClient.RequestType.CLEAN_STATEFUL_CHECKOINTS);

    runClient(simpleCheckpointManagerClient);
    finishedSignal.await(2, TimeUnit.SECONDS);

    verify(backendStorage).dispose(anyString(), anyString(), anyBoolean());

    Message response = simpleCheckpointManagerClient.getResponse();
    assertTrue(response instanceof CheckpointManager.CleanStatefulCheckpointResponse);
    assertEquals(StatusCode.OK, simpleCheckpointManagerClient.getResponseStatus());
  }

  @Test
  public void testRegiseterTMaster() throws Exception {
    prepareClient(SimpleCheckpointManagerClient.RequestType.REGISTER_TMASTER);

    runClient(simpleCheckpointManagerClient);
    finishedSignal.await(2, TimeUnit.SECONDS);

    Message response = simpleCheckpointManagerClient.getResponse();

    assertTrue(response instanceof CheckpointManager.RegisterTMasterResponse);
    assertEquals(StatusCode.OK, simpleCheckpointManagerClient.getResponseStatus());
  }

  @Test
  public void testRegisterStmgr() throws Exception {
    prepareClient(SimpleCheckpointManagerClient.RequestType.REGISTER_STMGR);

    runClient(simpleCheckpointManagerClient);
    finishedSignal.await(2, TimeUnit.SECONDS);

    Message response = simpleCheckpointManagerClient.getResponse();
    assertTrue(response instanceof CheckpointManager.RegisterStMgrResponse);
    assertEquals(StatusCode.OK, simpleCheckpointManagerClient.getResponseStatus());
  }

  private void prepareClient(SimpleCheckpointManagerClient.RequestType requestType) {
    simpleCheckpointManagerClient =
        new SimpleCheckpointManagerClient(clientLooper,
            SERVER_HOST,
            serverPort,
            finishedSignal,
            requestType);
  }

  private void runServer() {
    Runnable runServer = new Runnable() {
      @Override
      public void run() {
        checkpointManagerServer.start();
        checkpointManagerServer.getNIOLooper().loop();
      }
    };
    threadPools.execute(runServer);
  }

  private void runClient(final SimpleCheckpointManagerClient client) {
    Runnable runClient = new Runnable() {
      @Override
      public void run() {
        client.start();
        client.getNIOLooper().loop();
      }
    };
    threadPools.execute(runClient);
  }

  private static class SimpleCheckpointManagerClient extends HeronClient {
    private RequestType requestType;
    private Message response;
    private StatusCode responseStatus;
    private CountDownLatch finishedSignal;

    public enum RequestType {
      SAVE_INSTANCE_STATE,
      GET_INSTANCE_STATE,
      CLEAN_STATEFUL_CHECKOINTS,
      REGISTER_STMGR,
      REGISTER_TMASTER
    }

    SimpleCheckpointManagerClient(NIOLooper looper, String host,
                                  int port, CountDownLatch finishedSignal,
                                  RequestType requestType) {
      super(looper, host, port,
          new HeronSocketOptions(100 * 1024 * 1024, 100,
              100 * 1024 * 1024, 100,
              5 * 1024 * 1024,
              5 * 1024 * 1024));

      this.finishedSignal = finishedSignal;
      this.requestType = requestType;
      this.responseStatus = StatusCode.TIMEOUT_ERROR;
    }

    @Override
    public void onConnect(StatusCode status) {
      if (status != StatusCode.OK) {
        org.junit.Assert.fail("Connection with server failed");
      } else {
        switch (requestType) {
          case SAVE_INSTANCE_STATE:
            sendSaveInstanceStateRequest(saveInstanceStateRequest);
            break;
          case GET_INSTANCE_STATE:
            sendGetInstanceStateRequest(getInstanceStateRequest);
            break;
          case CLEAN_STATEFUL_CHECKOINTS:
            sendCleanStatefulCheckpointRequest(cleanStatefulCheckpointRequest);
            break;
          case REGISTER_STMGR:
            sendRegisterStMgrRequest(registerStmgrRequest);
            break;
          case REGISTER_TMASTER:
            sendRegisterTMasterRequest(registerTMasterRequest);
            break;
          default:
            break;
        }
      }
    }

    @Override
    public void onError() {
      org.junit.Assert.fail("Error in client while talking to server");
    }

    @Override
    public void onClose() {

    }

    public void sendSaveInstanceStateRequest(CheckpointManager.SaveInstanceStateRequest request) {
      sendRequest(request, CheckpointManager.SaveInstanceStateResponse.newBuilder());
    }

    public void sendGetInstanceStateRequest(CheckpointManager.GetInstanceStateRequest request) {
      sendRequest(request, CheckpointManager.GetInstanceStateResponse.newBuilder());
    }

    public void sendCleanStatefulCheckpointRequest(
        CheckpointManager.CleanStatefulCheckpointRequest request) {
      sendRequest(request, CheckpointManager.CleanStatefulCheckpointResponse.newBuilder());
    }

    public void sendRegisterTMasterRequest(CheckpointManager.RegisterTMasterRequest request) {
      sendRequest(request, CheckpointManager.RegisterTMasterResponse.newBuilder());
    }

    public void sendRegisterStMgrRequest(CheckpointManager.RegisterStMgrRequest request) {
      sendRequest(request, CheckpointManager.RegisterStMgrResponse.newBuilder());
    }

    public StatusCode getResponseStatus() {
      return this.responseStatus;
    }

    public Message getResponse() {
      return this.response;
    }

    @Override
    public void onResponse(StatusCode status, Object ctx, Message aResponse) {
      this.responseStatus = status;
      this.response = aResponse;
      this.finishedSignal.countDown();
    }

    @Override
    public void onIncomingMessage(Message request) {
      org.junit.Assert.fail("Expected message from client");
    }
  }
}
