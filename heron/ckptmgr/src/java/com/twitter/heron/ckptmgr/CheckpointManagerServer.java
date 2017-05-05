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
import java.nio.channels.SocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import com.twitter.heron.common.basics.NIOLooper;
import com.twitter.heron.common.network.HeronServer;
import com.twitter.heron.common.network.HeronSocketOptions;
import com.twitter.heron.common.network.REQID;
import com.twitter.heron.proto.ckptmgr.CheckpointManager;
import com.twitter.heron.proto.system.Common;
import com.twitter.heron.spi.statefulstorage.Checkpoint;
import com.twitter.heron.spi.statefulstorage.IStatefulStorage;

public class CheckpointManagerServer extends HeronServer {
  private static final Logger LOG = Logger.getLogger(CheckpointManagerServer.class.getName());

  private final String topologyName;
  private final String topologyId;
  private final String checkpointMgrId;
  private final IStatefulStorage checkpointsBackend;

  private SocketChannel connection;

  public CheckpointManagerServer(
      String topologyName, String topologyId, String checkpointMgrId,
      IStatefulStorage checkpointsBackend, NIOLooper s, String host,
      int port, HeronSocketOptions options) {
    super(s, host, port, options);

    this.topologyName = topologyName;
    this.topologyId = topologyId;
    this.checkpointMgrId = checkpointMgrId;
    this.checkpointsBackend = checkpointsBackend;

    this.connection = null;

    registerInitialization();
  }

  private void registerInitialization() {
    registerOnRequest(CheckpointManager.RegisterStMgrRequest.newBuilder());

    registerOnRequest(CheckpointManager.RegisterTMasterRequest.newBuilder());

    registerOnRequest(CheckpointManager.SaveInstanceStateRequest.newBuilder());

    registerOnRequest(CheckpointManager.GetInstanceStateRequest.newBuilder());

    registerOnRequest(CheckpointManager.CleanStatefulCheckpointRequest.newBuilder());
  }

  @Override
  public void onConnect(SocketChannel channel) {
    LOG.info("Got a new connection from host:port "
        + channel.socket().getRemoteSocketAddress());
  }

  @Override
  public void onRequest(REQID rid, SocketChannel channel, Message request) {
    if (request instanceof CheckpointManager.RegisterStMgrRequest) {
      handleStMgrRegisterRequest(rid, channel, (CheckpointManager.RegisterStMgrRequest) request);
    } else if (request instanceof CheckpointManager.RegisterTMasterRequest) {
      handleTMasterRegisterRequest(rid, channel,
                                   (CheckpointManager.RegisterTMasterRequest) request);
    } else if (request instanceof CheckpointManager.SaveInstanceStateRequest) {
      handleSaveInstanceStateRequest(
          rid, channel, (CheckpointManager.SaveInstanceStateRequest) request);
    } else if (request instanceof CheckpointManager.GetInstanceStateRequest) {
      handleGetInstanceStateRequest(
          rid, channel, (CheckpointManager.GetInstanceStateRequest) request);
    } else if (request instanceof CheckpointManager.CleanStatefulCheckpointRequest) {
      handleCleanStatefulCheckpointRequest(
          rid, channel, (CheckpointManager.CleanStatefulCheckpointRequest) request);
    } else {
      LOG.severe("Unknown kind of request: " + request.getClass().getName());
    }
  }

  protected void handleCleanStatefulCheckpointRequest(
      REQID rid,
      SocketChannel channel,
      CheckpointManager.CleanStatefulCheckpointRequest request
  ) {
    LOG.info("Got a clean request from " + request.toString() + " host:port "
        + channel.socket().getRemoteSocketAddress());

    boolean deleteAll = request.hasCleanAllCheckpoints() && request.getCleanAllCheckpoints();
    boolean res = checkpointsBackend.dispose(topologyName,
                                             request.getOldestCheckpointPreserved(), deleteAll);
    Common.StatusCode statusCode = res ? Common.StatusCode.OK : Common.StatusCode.NOTOK;

    CheckpointManager.CleanStatefulCheckpointResponse.Builder responseBuilder =
        CheckpointManager.CleanStatefulCheckpointResponse.newBuilder();
    responseBuilder.setStatus(Common.Status.newBuilder().setStatus(statusCode));

    if (res) {
      LOG.info("Dispose checkpoint successful");
    } else {
      LOG.info("Dispose checkpoint not successful");
    }

    sendResponse(rid, channel, responseBuilder.build());
  }

  protected void handleTMasterRegisterRequest(
      REQID rid,
      SocketChannel channel,
      CheckpointManager.RegisterTMasterRequest request
  ) {
    LOG.info("Got a register request from TMaster host:port "
        + channel.socket().getRemoteSocketAddress());

    CheckpointManager.RegisterTMasterResponse.Builder responseBuilder =
        CheckpointManager.RegisterTMasterResponse.newBuilder();

    if (!request.getTopologyName().equals(topologyName)) {
      LOG.severe("The register message was from a different topology: "
          + request.getTopologyName());
      responseBuilder.setStatus(Common.Status.newBuilder().setStatus(Common.StatusCode.NOTOK));
    } else if (!request.getTopologyId().equals(topologyId)) {
      LOG.severe("The register message was from a different topology id: "
          + request.getTopologyName());
      responseBuilder.setStatus(Common.Status.newBuilder().setStatus(Common.StatusCode.NOTOK));
    } else if (connection != null) {
      // TODO(mfu): Should we do this?
      LOG.warning("We already have an active connection from the tmaster "
          + "Closing existing connection...");

      try {
        connection.close();
      } catch (IOException e) {
        LOG.error("Failed to close connection from: "
                  + connection.socket().getRemoteSocketAddress());
      }

      connection = null;
      responseBuilder.setStatus(Common.Status.newBuilder().setStatus(Common.StatusCode.NOTOK));
    } else {
      connection = channel;
      responseBuilder.setStatus(Common.Status.newBuilder().setStatus(Common.StatusCode.OK));
    }

    sendResponse(rid, channel, responseBuilder.build());
  }

  protected void handleStMgrRegisterRequest(
      REQID rid,
      SocketChannel channel,
      CheckpointManager.RegisterStMgrRequest request
  ) {
    LOG.info("Got a register request from " + request.getStmgrId() + " host:port "
        + channel.socket().getRemoteSocketAddress());

    CheckpointManager.RegisterStMgrResponse.Builder responseBuilder =
        CheckpointManager.RegisterStMgrResponse.newBuilder();

    if (!request.getTopologyName().equals(topologyName)) {
      LOG.severe("The register message was from a different topology: "
          + request.getTopologyName());
      responseBuilder.setStatus(Common.Status.newBuilder().setStatus(Common.StatusCode.NOTOK));
    } else if (!request.getTopologyId().equals(topologyId)) {
      LOG.severe("The register message was from a different topology id: "
          + request.getTopologyName());
      responseBuilder.setStatus(Common.Status.newBuilder().setStatus(Common.StatusCode.NOTOK));
    } else if (connection != null) {
      // TODO(mfu): Should we do this?
      LOG.warning("We already have an active connection from the stmgr "
          + request.getStmgrId() + ". Closing existing connection...");

      try {
        connection.close();
      } catch (IOException e) {
        LOG.error("Failed to close connection from: "
                  + connection.socket().getRemoteSocketAddress());
      }

      connection = null;
      responseBuilder.setStatus(Common.Status.newBuilder().setStatus(Common.StatusCode.NOTOK));
    } else {
      connection = channel;
      responseBuilder.setStatus(Common.Status.newBuilder().setStatus(Common.StatusCode.OK));
    }

    sendResponse(rid, channel, responseBuilder.build());
  }

  protected void handleSaveInstanceStateRequest(
      REQID rid,
      SocketChannel channel,
      CheckpointManager.SaveInstanceStateRequest request
  ) {
    Checkpoint checkpoint = new Checkpoint(topologyName, request);
    LOG.info("Got a save checkpoint request for " + checkpoint.getCheckpointId() + " "
        + checkpoint.getComponent() + " " + checkpoint.getInstance() + " on connection: "
        + channel.socket().getRemoteSocketAddress());

    boolean res = checkpointsBackend.store(checkpoint);
    Common.StatusCode statusCode = res ? Common.StatusCode.OK : Common.StatusCode.NOTOK;

    CheckpointManager.SaveInstanceStateResponse.Builder responseBuilder =
        CheckpointManager.SaveInstanceStateResponse.newBuilder();
    responseBuilder.setStatus(Common.Status.newBuilder().setStatus(statusCode));
    responseBuilder.setCheckpointId(request.getCheckpoint().getCheckpointId());
    responseBuilder.setInstance(request.getInstance());

    if (res) {
      LOG.info("Save checkpoint successful for " + checkpoint.getCheckpointId() + " "
          + checkpoint.getComponent() + " " + checkpoint.getInstance());
    } else {
      LOG.info("Save checkpoint not successful for " + checkpoint.getCheckpointId() + " "
          + checkpoint.getComponent() + " " + checkpoint.getInstance());
    }

    sendResponse(rid, channel, responseBuilder.build());
  }

  protected void handleGetInstanceStateRequest(
      REQID rid,
      SocketChannel channel,
      CheckpointManager.GetInstanceStateRequest request
  ) {
    Checkpoint checkpoint = new Checkpoint(topologyName, request);
    LOG.info("Got a get checkpoint request for " + checkpoint.getCheckpointId() + " "
        + checkpoint.getComponent() + " " + checkpoint.getInstance() + " on connection: "
        + channel.socket().getRemoteSocketAddress());

    CheckpointManager.GetInstanceStateResponse.Builder responseBuilder =
        CheckpointManager.GetInstanceStateResponse.newBuilder();
    responseBuilder.setInstance(request.getInstance());
    responseBuilder.setCheckpointId(request.getCheckpointId());

    boolean res;
    if (!request.hasCheckpointId() || request.getCheckpointId().isEmpty()) {
      res = true;

      LOG.info("The checkpoint id was empty, this sending empty state");
      CheckpointManager.InstanceStateCheckpoint dummyState =
          CheckpointManager.InstanceStateCheckpoint.newBuilder()
              .setCheckpointId(request.getCheckpointId())
              .setState(ByteString.EMPTY).build();

      responseBuilder.setCheckpoint(dummyState);
    } else {
      res = checkpointsBackend.restore(checkpoint);

      if (res) {
        LOG.info("Get checkpoint successful for " + checkpoint.getCheckpointId() + " "
            + checkpoint.getComponent() + " " + checkpoint.getInstance());

        // Set the checkpoint-state in response
        responseBuilder.setCheckpoint(checkpoint.checkpoint().getCheckpoint());
      } else {
        LOG.info("Get checkpoint not successful for " + checkpoint.getCheckpointId() + " "
            + checkpoint.getComponent() + " " + checkpoint.getInstance());
      }
    }

    Common.StatusCode statusCode = res ? Common.StatusCode.OK : Common.StatusCode.NOTOK;
    responseBuilder.setStatus(Common.Status.newBuilder().setStatus(statusCode));

    sendResponse(rid, channel, responseBuilder.build());
  }

  @Override
  public void onMessage(SocketChannel channel, Message message) {

  }

  @Override
  public void onClose(SocketChannel channel) {
    LOG.log(Level.SEVERE, "Got a connection close from remote socket address: {0}",
        new Object[]{channel.socket().getRemoteSocketAddress()});

    // Reset the connection
    connection = null;
  }
}
