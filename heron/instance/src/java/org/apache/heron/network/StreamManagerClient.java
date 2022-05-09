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

package org.apache.heron.network;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;

import org.apache.heron.common.basics.Communicator;
import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.basics.SingletonRegistry;
import org.apache.heron.common.config.SystemConfig;
import org.apache.heron.common.network.HeronClient;
import org.apache.heron.common.network.HeronSocketOptions;
import org.apache.heron.common.network.StatusCode;
import org.apache.heron.common.utils.misc.PhysicalPlanHelper;
import org.apache.heron.instance.InstanceControlMsg;
import org.apache.heron.metrics.GatewayMetrics;
import org.apache.heron.proto.ckptmgr.CheckpointManager;
import org.apache.heron.proto.stmgr.StreamManager;
import org.apache.heron.proto.system.Common;
import org.apache.heron.proto.system.HeronTuples;
import org.apache.heron.proto.system.PhysicalPlans;

/**
 * StreamClient implements SocketClient and communicate with Stream Manager, it will:
 * 1. Register the message of NewInstanceAssignmentMessage and HeronTupleSet2.
 * 2. Send Register Request when it is onConnect()
 * 3. Handle relative response for requests
 * 4. if onIncomingMessage(message) is called, it will see whether it is NewAssignment or NewTuples.
 * 5. If it is a new assignment, it will pass the PhysicalPlan to Executor,
 * which will new a corresponding instance.
 */

public class StreamManagerClient extends HeronClient {
  private static final Logger LOG = Logger.getLogger(StreamManagerClient.class.getName());

  private final String topologyName;
  private final String topologyId;

  private final PhysicalPlans.Instance instance;

  // For spout, it will buffer Control tuple, while for bolt, it will buffer data tuple.
  private final Communicator<Message> inStreamQueue;

  private final Communicator<Message> outStreamQueue;

  private final Communicator<InstanceControlMsg> inControlQueue;

  private final GatewayMetrics gatewayMetrics;

  private final SystemConfig systemConfig;

  private PhysicalPlanHelper helper;

  private long lastNotConnectedLogTime = 0;

  public StreamManagerClient(NIOLooper s, String streamManagerHost, int streamManagerPort,
                             String topologyName, String topologyId,
                             PhysicalPlans.Instance instance,
                             Communicator<Message> inStreamQueue,
                             Communicator<Message> outStreamQueue,
                             Communicator<InstanceControlMsg> inControlQueue,
                             HeronSocketOptions options,
                             GatewayMetrics gatewayMetrics) {
    super(s, streamManagerHost, streamManagerPort, options);

    this.topologyName = topologyName;
    this.topologyId = topologyId;

    this.instance = instance;
    this.inStreamQueue = inStreamQueue;
    this.outStreamQueue = outStreamQueue;
    this.inControlQueue = inControlQueue;

    this.systemConfig =
        (SystemConfig) SingletonRegistry.INSTANCE.getSingleton(SystemConfig.HERON_SYSTEM_CONFIG);

    this.gatewayMetrics = gatewayMetrics;

    addStreamManagerClientTasksOnWakeUp();
  }

  private void addStreamManagerClientTasksOnWakeUp() {
    Runnable task = new Runnable() {
      @Override
      public void run() {
        sendStreamMessageIfNeeded();
        readStreamMessageIfNeeded();
      }
    };
    getNIOLooper().addTasksOnWakeup(task);
  }

  private void registerMessagesToHandle() {
    registerOnMessage(StreamManager.NewInstanceAssignmentMessage.newBuilder());
    registerOnMessage(HeronTuples.HeronTupleSet2.newBuilder());

    // Register stateful processing related messages
    registerOnMessage(CheckpointManager.InitiateStatefulCheckpoint.newBuilder());
    registerOnMessage(CheckpointManager.RestoreInstanceStateRequest.newBuilder());
    registerOnMessage(CheckpointManager.StartInstanceStatefulProcessing.newBuilder());
    registerOnMessage(CheckpointManager.StatefulConsistentCheckpointSaved.newBuilder());
  }


  @Override
  public void onError() {
    LOG.severe("Disconnected from Stream Manager.");

    // We would set PhysicalPlanHelper to null onError(),
    // since we would re-connect to stream manager and wait for new PhysicalPlan
    // the stream manager publishes
    LOG.info("Clean the old PhysicalPlanHelper in StreamManagerClient.");
    helper = null;

    // Dispatch to onConnect(...)
    onConnect(StatusCode.CONNECT_ERROR);
  }

  @Override
  public void onConnect(StatusCode status) {
    if (status != StatusCode.OK) {
      LOG.log(Level.WARNING,
          "Error connecting to Stream Manager with status: {0}, Retrying...", status);
      Runnable r = new Runnable() {
        public void run() {
          start();
        }
      };
      getNIOLooper().registerTimerEvent(
          systemConfig.getInstanceReconnectStreammgrInterval(), r);
      return;
    }

    // Initialize the register: determine what messages we would like to handle
    registerMessagesToHandle();

    // Build the request and send it.
    LOG.info("Connected to Stream Manager. Ready to send register request");
    sendRegisterRequest();
  }

  // Build register request and send to stream mgr
  private void sendRegisterRequest() {
    StreamManager.RegisterInstanceRequest request =
        StreamManager.RegisterInstanceRequest.newBuilder().
            setInstance(instance).setTopologyName(topologyName).setTopologyId(topologyId).
            build();

    // The timeout would be the reconnect-interval-seconds
    sendRequest(request, null,
        StreamManager.RegisterInstanceResponse.newBuilder(),
        systemConfig.getInstanceReconnectStreammgrInterval());
  }

  @Override
  public void onResponse(StatusCode status, Object ctx, Message response) {
    if (status != StatusCode.OK) {
      //TODO:- is this a good thing?
      throw new RuntimeException("Response from Stream Manager not ok");
    }
    if (response instanceof StreamManager.RegisterInstanceResponse) {
      handleRegisterResponse((StreamManager.RegisterInstanceResponse) response);
    } else {
      throw new RuntimeException("Unknown kind of response received from Stream Manager");
    }
  }


  @Override
  public void onIncomingMessage(Message message) {
    gatewayMetrics.updateReceivedPacketsCount(1);
    gatewayMetrics.updateReceivedPacketsSize(message.getSerializedSize());

    if (message instanceof StreamManager.NewInstanceAssignmentMessage) {
      StreamManager.NewInstanceAssignmentMessage m =
          (StreamManager.NewInstanceAssignmentMessage) message;
      LOG.info("Handling assignment message from direct NewInstanceAssignmentMessage");
      handleAssignmentMessage(m.getPplan());
    } else if (message instanceof HeronTuples.HeronTupleSet2) {
      handleNewTuples2((HeronTuples.HeronTupleSet2) message);
    } else if (message instanceof CheckpointManager.InitiateStatefulCheckpoint)  {
      handleCheckpointRequest((CheckpointManager.InitiateStatefulCheckpoint) message);
    } else if (message instanceof CheckpointManager.RestoreInstanceStateRequest) {
      handleRestoreInstanceStateRequest((CheckpointManager.RestoreInstanceStateRequest) message);
    } else if (message instanceof CheckpointManager.StartInstanceStatefulProcessing) {
      handleStartStatefulRequest((CheckpointManager.StartInstanceStatefulProcessing) message);
    } else if (message instanceof CheckpointManager.StatefulConsistentCheckpointSaved) {
      handleCheckpointSaved((CheckpointManager.StatefulConsistentCheckpointSaved) message);
    } else {
      throw new RuntimeException("Unknown kind of message received from Stream Manager");
    }
  }

  @Override
  public void onClose() {
    LOG.info("StreamManagerClient exits.");
  }

  // Send out all the data
  public void sendAllMessage() {
    if (!isConnected()) {
      return;
    }

    LOG.info("Flushing all pending data in StreamManagerClient");
    // Collect all tuples in queue
    int size = outStreamQueue.size();
    for (int i = 0; i < size; i++) {
      Message streamMessage = outStreamQueue.poll();

      sendMessage(streamMessage);
    }
  }

  private void sendStreamMessageIfNeeded() {
    if (isStreamMgrReadyReceiveTuples()) {
      if (getOutstandingPackets() <= 0) {
        // In order to avoid packets back up in Client side,
        // We would poll message from queue and send them only when there are no outstanding packets
        while (!outStreamQueue.isEmpty()) {
          Message tupleSet = outStreamQueue.poll();

          gatewayMetrics.updateSentPacketsCount(1);
          gatewayMetrics.updateSentPacketsSize(tupleSet.getSerializedSize());
          sendMessage(tupleSet);
        }
      }

      if (!outStreamQueue.isEmpty()) {
        // We still have messages to send
        startWriting();
      }
    } else {
      LOG.info("Stop writing due to not yet connected to Stream Manager.");
    }
  }

  private void readStreamMessageIfNeeded() {
    final long lastNotConnectedLogThrottleSeconds = 5;

    // If client is not connected, just return
    if (isConnected()) {
      if (isInQueuesAvailable() || helper == null) {
        startReading();
      } else {
        gatewayMetrics.updateInQueueFullCount();
        stopReading();
      }
    } else {
      long now = System.currentTimeMillis();
      if (now - lastNotConnectedLogTime > lastNotConnectedLogThrottleSeconds * 1000) {
        LOG.info(String.format("Stop reading due to not yet connected to Stream Manager. This "
            + "message is throttled to emit no more than once every %d seconds.",
            lastNotConnectedLogThrottleSeconds));
        lastNotConnectedLogTime = now;
      }
    }
  }

  private void handleStartStatefulRequest(
      CheckpointManager.StartInstanceStatefulProcessing request) {
    LOG.info("Received a StartInstanceStatefulProcessing request: " + request);

    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder()
        .setStartInstanceStatefulProcessing(request)
        .build();
    inControlQueue.offer(instanceControlMsg);
  }

  private void handleCheckpointSaved(
      CheckpointManager.StatefulConsistentCheckpointSaved message) {
    LOG.info("Received a StatefulCheckpointSaved message with checkpoint id: "
        + message.getConsistentCheckpoint().getCheckpointId());

    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder()
        .setStatefulCheckpointSaved(message)
        .build();
    inControlQueue.offer(instanceControlMsg);
  }

  private void handleRestoreInstanceStateRequest(
      CheckpointManager.RestoreInstanceStateRequest request) {
    LOG.info("Received a RestoreInstanceState request with checkpoint id: "
          + request.getState().getCheckpointId());

    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder()
        .setRestoreInstanceStateRequest(request)
        .build();
    inControlQueue.offer(instanceControlMsg);
  }

  private void handleCheckpointRequest(
      CheckpointManager.InitiateStatefulCheckpoint request) {
    LOG.info("Handling instance checkpoint request: " + request);
    inStreamQueue.offer(request);
  }

  private void handleRegisterResponse(StreamManager.RegisterInstanceResponse response) {
    if (response.getStatus().getStatus() != Common.StatusCode.OK) {
      throw new RuntimeException("Stream Manager returned a not ok response for register");
    }
    LOG.info("We registered ourselves to the Stream Manager");

    if (response.hasPplan()) {
      LOG.info("Handling assignment message from response");
      handleAssignmentMessage(response.getPplan());
    }
  }

  private void handleNewTuples2(HeronTuples.HeronTupleSet2 set) {
    HeronTuples.HeronTupleSet.Builder toFeed = HeronTuples.HeronTupleSet.newBuilder();
    // Set the source task id
    toFeed.setSrcTaskId(set.getSrcTaskId());

    if (set.hasControl()) {
      toFeed.setControl(set.getControl());
    } else {
      // Either control or data
      HeronTuples.HeronDataTupleSet.Builder builder = HeronTuples.HeronDataTupleSet.newBuilder();
      builder.setStream(set.getData().getStream());
      try {
        for (ByteString bs : set.getData().getTuplesList()) {
          builder.addTuples(HeronTuples.HeronDataTuple.parseFrom(bs));
        }
      } catch (InvalidProtocolBufferException e) {
        LOG.log(Level.SEVERE, "Failed to parse protobuf", e);
      }
      toFeed.setData(builder);
    }

    HeronTuples.HeronTupleSet s = toFeed.build();
    inStreamQueue.offer(s);
  }

  private void handleAssignmentMessage(PhysicalPlans.PhysicalPlan pplan) {
    LOG.fine("Physical Plan: " + pplan);
    PhysicalPlanHelper newHelper = new PhysicalPlanHelper(pplan, instance.getInstanceId());

    if (helper != null && (!helper.getMyComponent().equals(newHelper.getMyComponent())
        || helper.getMyTaskId() != newHelper.getMyTaskId())) {
      // Right now if we already are something, and the stmgr tell us to
      // change our role, we just exit. When we come back up again
      // we will get the new assignment
      throw new RuntimeException("Our Assignment has changed. We will die to pick it");
    }

    if (helper == null) {
      LOG.info("We received a new Physical Plan.");
    } else {
      LOG.info("We received a new Physical Plan with same assignment. Should be state changes.");
      LOG.info(String.format("Old state: %s; new sate: %s.",
          helper.getTopologyState(), newHelper.getTopologyState()));
    }
    helper = newHelper;
    LOG.info("Push to Executor");
    InstanceControlMsg instanceControlMsg = InstanceControlMsg.newBuilder().
        setNewPhysicalPlanHelper(helper).
        build();

    inControlQueue.offer(instanceControlMsg);
  }

  private boolean isStreamMgrReadyReceiveTuples() {
    // The Stream Manager is ready only when:
    // 1. We could connect to it
    // 2. We receive the PhysicalPlan published by Stream Manager
    return isConnected() && helper != null;
  }

  // Return true if we could offer item to the inStreamQueue
  private boolean isInQueuesAvailable() {
    return inStreamQueue.size() < inStreamQueue.getExpectedAvailableCapacity();
  }
}
