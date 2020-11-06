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

package org.apache.heron.ckptmgr;

import java.io.IOException;

import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Message;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.heron.api.generated.TopologyAPI;
import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.common.network.HeronClient;
import org.apache.heron.common.network.StatusCode;
import org.apache.heron.common.testhelpers.HeronServerTester;
import org.apache.heron.proto.ckptmgr.CheckpointManager;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.spi.statefulstorage.Checkpoint;
import org.apache.heron.spi.statefulstorage.CheckpointInfo;
import org.apache.heron.spi.statefulstorage.IStatefulStorage;

import static org.apache.heron.common.testhelpers.HeronServerTester.RESPONSE_RECEIVED_TIMEOUT;
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

  private static CheckpointManager.InstanceStateCheckpoint checkpointPartition;
  private static CheckpointManager.CheckpointComponentMetadata checkpointComponentMetadata;
  private static CheckpointManager.SaveInstanceStateRequest saveInstanceStateRequest;
  private static CheckpointManager.GetInstanceStateRequest getInstanceStateRequest;
  private static CheckpointManager.CleanStatefulCheckpointRequest cleanStatefulCheckpointRequest;
  private static CheckpointManager.RegisterStMgrRequest registerStmgrRequest;
  private static CheckpointManager.RegisterTManagerRequest registerTManagerRequest;

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
    final String TOPO_ID = "topo_id";
    final String TOPO_NAME = "topo_name";

    PhysicalPlans.InstanceInfo info = PhysicalPlans.InstanceInfo.newBuilder()
        .setTaskId(TASK_ID)
        .setComponentIndex(COMPONENT_INDEX)
        .setComponentName(COMPONENT_NAME)
        .build();

    TopologyAPI.Topology topology = TopologyAPI.Topology.newBuilder()
        .setId(TOPO_ID)
        .setName(TOPO_NAME)
        .setState(TopologyAPI.TopologyState.RUNNING)
        .build();

    PhysicalPlans.PhysicalPlan pplan = PhysicalPlans.PhysicalPlan.newBuilder()
        .setTopology(topology)
        .build();

    instance = PhysicalPlans.Instance.newBuilder()
        .setInstanceId(INSTANCE_ID)
        .setStmgrId(STMGR_ID)
        .setInfo(info)
        .build();

    checkpointPartition = CheckpointManager.InstanceStateCheckpoint.newBuilder()
        .setCheckpointId(CHECKPOINT_ID)
        .setState(ByteString.copyFrom(BYTES))
        .build();

    checkpointComponentMetadata = CheckpointManager.CheckpointComponentMetadata.newBuilder()
        .setComponentName(COMPONENT_NAME)
        .setParallelism(2)
        .build();

    saveInstanceStateRequest = CheckpointManager.SaveInstanceStateRequest.newBuilder()
        .setInstance(instance)
        .setCheckpoint(checkpointPartition)
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
        .setPhysicalPlan(pplan)
        .build();

    registerTManagerRequest = CheckpointManager.RegisterTManagerRequest.newBuilder()
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
                verify(statefulStorage).storeCheckpoint(
                    any(CheckpointInfo.class), any(Checkpoint.class));
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
    final CheckpointInfo info = new CheckpointInfo(CHECKPOINT_ID, instance);
    final Checkpoint checkpoint = new Checkpoint(checkpointPartition);
    when(statefulStorage.restoreCheckpoint(any(CheckpointInfo.class)))
        .thenReturn(checkpoint);

    runTest(TestRequestHandler.RequestType.GET_INSTANCE_STATE,
        new HeronServerTester.SuccessResponseHandler(
            CheckpointManager.GetInstanceStateResponse.class,
            new HeronServerTester.TestResponseHandler() {
              @Override
              public void handleResponse(HeronClient client, StatusCode status,
                                         Object ctx, Message response) throws Exception {
                verify(statefulStorage).restoreCheckpoint(info);
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
                verify(statefulStorage).dispose(anyString(), anyBoolean());
              }
            })
    );
  }

  @Test
  public void testRegisterTManager() throws Exception {
    runTest(TestRequestHandler.RequestType.REGISTER_TMANAGER,
        new HeronServerTester.SuccessResponseHandler(
            CheckpointManager.RegisterTManagerResponse.class));
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
      REGISTER_TMANAGER(registerTManagerRequest,
          CheckpointManager.RegisterTManagerResponse.getDescriptor());

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
