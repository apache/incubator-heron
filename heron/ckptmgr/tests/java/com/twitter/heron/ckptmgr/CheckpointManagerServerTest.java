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

import java.io.IOException;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.basics.SysUtils;
import com.twitter.heron.common.network.HeronClient;
import com.twitter.heron.common.network.StatusCode;
import com.twitter.heron.common.testhelpers.HeronServerTester;
import com.twitter.heron.proto.ckptmgr.CheckpointManager;
import com.twitter.heron.proto.system.PhysicalPlans;
import com.twitter.heron.spi.statefulstorage.Checkpoint;
import com.twitter.heron.spi.statefulstorage.IStatefulStorage;

import static com.twitter.heron.common.testhelpers.HeronServerTester.RESPONSE_RECEIVED_TIMEOUT;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class CheckpointManagerServerTest {

  private static final String TOPOLOGY_NAME = "topology_name";
  private static final String TOPOLOGY_ID = "topology_id";
  private static final String CHECKPOINT_ID = "checkpoint_id";
  private static final String CHECKPOINT_MANAGER_ID = "ckptmgr_id";

  private static CheckpointManager.InstanceStateCheckpoint instanceStateCheckpoint;
  private static CheckpointManager.SaveInstanceStateRequest saveInstanceStateRequest;
  private static CheckpointManager.GetInstanceStateRequest getInstanceStateRequest;
  private static CheckpointManager.CleanStatefulCheckpointRequest cleanStatefulCheckpointRequest;
  private static CheckpointManager.RegisterStMgrRequest registerStmgrRequest;
  private static CheckpointManager.RegisterTMasterRequest registerTMasterRequest;

  private static PhysicalPlans.Instance instance;

  private CheckpointManagerServer checkpointManagerServer;
  private IStatefulStorage statefulStorage;
  private HeronServerTester serverTester;

  @BeforeClass
  public static void setup() throws Exception {
    final String INSTANCE_ID = "instance_id";
    final String STMGR_ID = "stmgr_id";
    final int TASK_ID = 1;
    final int COMPONENT_INDEX = 1;
    final String COMPONENT_NAME = "component_name";
    final byte[] BYTES = "checkpoint manager server test bytes".getBytes();

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

    instanceStateCheckpoint = CheckpointManager.InstanceStateCheckpoint.newBuilder()
            .setCheckpointId(CHECKPOINT_ID)
            .setState(ByteString.copyFrom(BYTES))
            .build();

    saveInstanceStateRequest = CheckpointManager.SaveInstanceStateRequest.newBuilder()
        .setInstance(instance)
        .setCheckpoint(instanceStateCheckpoint)
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
    statefulStorage = mock(IStatefulStorage.class);

    checkpointManagerServer = new CheckpointManagerServer(TOPOLOGY_NAME, TOPOLOGY_ID,
        CHECKPOINT_MANAGER_ID, statefulStorage, new NIOLooper(), HeronServerTester.SERVER_HOST,
        SysUtils.getFreePort(), HeronServerTester.TEST_SOCKET_OPTIONS);
  }

  @After
  public void after() {
    serverTester.stop();
  }

  private void runTest(TestRequestHandler.RequestType requestType,
                       HeronServerTester.TestResponseHandler responseHandler)
      throws IOException, InterruptedException {
    serverTester = new HeronServerTester(checkpointManagerServer,
        new TestRequestHandler(requestType), responseHandler, RESPONSE_RECEIVED_TIMEOUT);
    serverTester.start();
  }

  @Test
  public void testSaveInstanceState() throws Exception {

    runTest(TestRequestHandler.RequestType.SAVE_INSTANCE_STATE,
        new HeronServerTester.SuccessResponseHandler(
            CheckpointManager.SaveInstanceStateResponse.class,
            new HeronServerTester.TestResponseHandler() {
              @Override
              public void handleResponse(HeronClient client, StatusCode status,
                                         Object ctx, Message response) throws Exception {
                verify(statefulStorage).store(any(Checkpoint.class));
                assertEquals(CHECKPOINT_ID,
                    ((CheckpointManager.SaveInstanceStateResponse) response).getCheckpointId());
                assertEquals(instance,
                    ((CheckpointManager.SaveInstanceStateResponse) response).getInstance());
              }
            })
    );
  }

  @Test
  public void testGetInstanceState() throws Exception {
    final Checkpoint checkpoint = new Checkpoint(TOPOLOGY_NAME, instance, instanceStateCheckpoint);
    when(statefulStorage.restore(TOPOLOGY_NAME, CHECKPOINT_ID, instance)).thenReturn(checkpoint);

    runTest(TestRequestHandler.RequestType.GET_INSTANCE_STATE,
        new HeronServerTester.SuccessResponseHandler(
            CheckpointManager.GetInstanceStateResponse.class,
            new HeronServerTester.TestResponseHandler() {
              @Override
              public void handleResponse(HeronClient client, StatusCode status,
                                         Object ctx, Message response) throws Exception {
                verify(statefulStorage).restore(TOPOLOGY_NAME, CHECKPOINT_ID, instance);
                assertEquals(checkpoint.getCheckpoint(),
                    ((CheckpointManager.GetInstanceStateResponse) response).getCheckpoint());
              }
            })
    );
  }

  @Test
  public void testCleanStatefulCheckpoint() throws Exception {
    runTest(TestRequestHandler.RequestType.CLEAN_STATEFUL_CHECKPOINTS,
        new HeronServerTester.SuccessResponseHandler(
            CheckpointManager.CleanStatefulCheckpointResponse.class,
            new HeronServerTester.TestResponseHandler() {
              @Override
              public void handleResponse(HeronClient client, StatusCode status,
                                         Object ctx, Message response) throws Exception {
                verify(statefulStorage).dispose(anyString(), anyString(), anyBoolean());
              }
            })
    );
  }

  @Test
  public void testRegisterTMaster() throws Exception {
    runTest(TestRequestHandler.RequestType.REGISTER_TMASTER,
        new HeronServerTester.SuccessResponseHandler(
            CheckpointManager.RegisterTMasterResponse.class));
  }

  @Test
  public void testRegisterStmgr() throws Exception {
    runTest(TestRequestHandler.RequestType.REGISTER_STMGR,
        new HeronServerTester.SuccessResponseHandler(
            CheckpointManager.RegisterStMgrResponse.class));
  }

  private static final class TestRequestHandler implements HeronServerTester.TestRequestHandler {
    private RequestType requestType;

    public enum RequestType {
      SAVE_INSTANCE_STATE(saveInstanceStateRequest,
          CheckpointManager.SaveInstanceStateResponse.getDescriptor()),
      GET_INSTANCE_STATE(getInstanceStateRequest,
          CheckpointManager.GetInstanceStateResponse.getDescriptor()),
      CLEAN_STATEFUL_CHECKPOINTS(cleanStatefulCheckpointRequest,
          CheckpointManager.CleanStatefulCheckpointResponse.getDescriptor()),
      REGISTER_STMGR(registerStmgrRequest,
          CheckpointManager.RegisterStMgrResponse.getDescriptor()),
      REGISTER_TMASTER(registerTMasterRequest,
          CheckpointManager.RegisterTMasterResponse.getDescriptor());

      private Message requestMessage;
      private Descriptors.Descriptor responseMessageDescriptor;

      RequestType(Message requestMessage, Descriptors.Descriptor responseMessageDescriptor) {
        this.requestMessage = requestMessage;
        this.responseMessageDescriptor = responseMessageDescriptor;
      }

      public Message getRequestMessage() {
        return requestMessage;
      }

      public Message.Builder newResponseBuilder() {
        return responseMessageDescriptor.toProto().newBuilderForType();
      }
    }

    private TestRequestHandler(RequestType requestType) {
      this.requestType = requestType;
    }

    @Override
    public Message getRequestMessage() {
      return requestType.getRequestMessage();
    }

    @Override
    public Message.Builder getResponseBuilder() {
      return requestType.newResponseBuilder();
    }
  }
}
