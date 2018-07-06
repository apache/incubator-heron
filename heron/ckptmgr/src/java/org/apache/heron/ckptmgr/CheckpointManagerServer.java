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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.channels.SocketChannel;
import java.nio.file.Files;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;
import com.google.protobuf.Message;

import org.apache.heron.common.basics.NIOLooper;
import org.apache.heron.common.network.HeronServer;
import org.apache.heron.common.network.HeronSocketOptions;
import org.apache.heron.common.network.REQID;
import org.apache.heron.proto.ckptmgr.CheckpointManager;
import org.apache.heron.proto.system.Common;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.spi.statefulstorage.Checkpoint;
import org.apache.heron.spi.statefulstorage.IStatefulStorage;
import org.apache.heron.spi.statefulstorage.StatefulStorageException;

public class CheckpointManagerServer extends HeronServer {
  private static final Logger LOG = Logger.getLogger(CheckpointManagerServer.class.getName());

  private final String topologyName;
  private final String topologyId;
  private final String checkpointMgrId;
  private final IStatefulStorage statefulStorage;

  private SocketChannel connection;

  public CheckpointManagerServer(
      String topologyName, String topologyId, String checkpointMgrId,
      IStatefulStorage statefulStorage, NIOLooper looper, String host,
      int port, HeronSocketOptions options) {
    super(looper, host, port, options);

    this.topologyName = topologyName;
    this.topologyId = topologyId;
    this.checkpointMgrId = checkpointMgrId;
    this.statefulStorage = statefulStorage;

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
    LOG.info(String.format("Got a clean request from %s running at host:port %s",
             request.toString(), channel.socket().getRemoteSocketAddress()));

    boolean deleteAll = request.hasCleanAllCheckpoints() && request.getCleanAllCheckpoints();
    Common.StatusCode statusCode = Common.StatusCode.OK;
    String errorMessage = "";

    try {
      statefulStorage.dispose(topologyName,
                                 request.getOldestCheckpointPreserved(), deleteAll);
      LOG.info("Dispose checkpoint successful");
    } catch (StatefulStorageException e) {
      errorMessage = String.format("Request to dispose checkpoint failed for oldest Checkpoint "
                                   + "%s and deleteAll? %b",
                                   request.getOldestCheckpointPreserved(), deleteAll);
      statusCode = Common.StatusCode.NOTOK;
      LOG.log(Level.WARNING, errorMessage, e);
    }

    CheckpointManager.CleanStatefulCheckpointResponse.Builder responseBuilder =
        CheckpointManager.CleanStatefulCheckpointResponse.newBuilder();
    responseBuilder.setStatus(Common.Status.newBuilder().setStatus(statusCode)
                              .setMessage(errorMessage));

    sendResponse(rid, channel, responseBuilder.build());
  }

  protected void handleTMasterRegisterRequest(
      REQID rid,
      SocketChannel channel,
      CheckpointManager.RegisterTMasterRequest request
  ) {
    LOG.info("Got a TMaster register request from TMaster host:port "
        + channel.socket().getRemoteSocketAddress());

    CheckpointManager.RegisterTMasterResponse.Builder responseBuilder =
        CheckpointManager.RegisterTMasterResponse.newBuilder();

    if (!checkRegistrationValidity(request.getTopologyName(),
                                   request.getTopologyId())) {
      String errorMessage = String.format("The TMaster register message came with a different "
                               + "topologyName: %s and/or topologyId: %s",
                               request.getTopologyName(),
                               request.getTopologyId());
      LOG.severe(errorMessage);
      responseBuilder.setStatus(Common.Status.newBuilder().setStatus(Common.StatusCode.NOTOK)
                                .setMessage(errorMessage));
    } else if (!checkExistingConnection()) {
      String errorMessage = "Please try again";
      responseBuilder.setStatus(Common.Status.newBuilder().setStatus(Common.StatusCode.NOTOK)
                                .setMessage(errorMessage));
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
    LOG.info(String.format("Got a StMgr register request from %s running on host:port %s",
                           request.getStmgrId(), channel.socket().getRemoteSocketAddress()));

    CheckpointManager.RegisterStMgrResponse.Builder responseBuilder =
        CheckpointManager.RegisterStMgrResponse.newBuilder();

    if (!checkRegistrationValidity(request.getTopologyName(),
                                   request.getTopologyId())) {
      String errorMessage = String.format("The StMgr register message came with a different "
                               + "topologyName: %s and/or topologyId: %s",
                               request.getTopologyName(),
                               request.getTopologyId());
      LOG.severe(errorMessage);
      responseBuilder.setStatus(Common.Status.newBuilder().setStatus(Common.StatusCode.NOTOK)
                                .setMessage(errorMessage));
    } else if (!checkExistingConnection()) {
      String errorMessage = "Please try again";
      responseBuilder.setStatus(Common.Status.newBuilder().setStatus(Common.StatusCode.NOTOK)
                                .setMessage(errorMessage));
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
    Checkpoint checkpoint = null;

    // TODO(nlu): handle states in different locations
    if (request.getCheckpoint().hasStateUri()) {
      checkpoint = loadCheckpoint(request.getInstance(), request.getCheckpoint().getCheckpointId(),
          request.getCheckpoint().getStateUri());
    } else {
      checkpoint = new Checkpoint(topologyName, request.getInstance(),
          request.getCheckpoint());
    }

    LOG.info(String.format("Got a save checkpoint request for checkpointId %s "
                           + " component %s instance %s on connection %s",
                           checkpoint.getCheckpointId(), checkpoint.getComponent(),
                           checkpoint.getInstance(), channel.socket().getRemoteSocketAddress()));

    Common.StatusCode statusCode = Common.StatusCode.OK;
    String errorMessage = "";
    try {
      statefulStorage.store(checkpoint);
      LOG.info(String.format("Saved checkpoint for checkpointId %s compnent %s instance %s",
                             checkpoint.getCheckpointId(), checkpoint.getComponent(),
                             checkpoint.getInstance()));
    } catch (StatefulStorageException e) {
      errorMessage = String.format("Save checkpoint not successful for checkpointId "
                                   + "%s component %s instance %s",
                                   checkpoint.getCheckpointId(), checkpoint.getComponent(),
                                   checkpoint.getInstance());
      statusCode = Common.StatusCode.NOTOK;
      LOG.log(Level.WARNING, errorMessage, e);
    }

    CheckpointManager.SaveInstanceStateResponse.Builder responseBuilder =
        CheckpointManager.SaveInstanceStateResponse.newBuilder();
    responseBuilder.setStatus(Common.Status.newBuilder().setStatus(statusCode)
                              .setMessage(errorMessage));
    responseBuilder.setCheckpointId(request.getCheckpoint().getCheckpointId());
    responseBuilder.setInstance(request.getInstance());

    sendResponse(rid, channel, responseBuilder.build());
  }

  private Checkpoint loadCheckpoint(PhysicalPlans.Instance instanceInfo,
                                    String checkpointId, String stateUri) {
    LOG.info("fetch state from: " + stateUri);
    CheckpointManager.InstanceStateCheckpoint checkpoint =
        CheckpointManager.InstanceStateCheckpoint.newBuilder()
            .setCheckpointId(checkpointId)
            .setState(loadStateFromFile(stateUri))
            .build();

    return new Checkpoint(topologyName, instanceInfo, checkpoint);
  }

  private ByteString loadStateFromFile(String stateUri) {
    File f = new File(stateUri);
    byte[] data = new byte[(int) f.length()];

    try (FileInputStream fis = new FileInputStream(stateUri);
         DataInputStream dis = new DataInputStream(fis)) {

      dis.read(data);
      return ByteString.copyFrom(data);
    } catch (IOException e) {
      throw new RuntimeException("failed to load local state", e);
    } finally {
      // we have to clean it here manually to avoid using too many disks
      // TODO(nlu): find a cleaner and more consistent way to handle temp state files
      if (f.exists()) {
        LOG.info("cleaning state file: " + f.getPath());
        f.delete();
      }
    }
  }

  protected void handleGetInstanceStateRequest(
      REQID rid,
      SocketChannel channel,
      CheckpointManager.GetInstanceStateRequest request
  ) {
    LOG.info(String.format("Got a get checkpoint request for checkpointId %s "
                           + " component %s taskId %d on connection %s",
                           request.getCheckpointId(),
                           request.getInstance().getInfo().getComponentName(),
                           request.getInstance().getInfo().getTaskId(),
                           channel.socket().getRemoteSocketAddress()));

    CheckpointManager.GetInstanceStateResponse.Builder responseBuilder =
        CheckpointManager.GetInstanceStateResponse.newBuilder();
    responseBuilder.setInstance(request.getInstance());
    responseBuilder.setCheckpointId(request.getCheckpointId());
    String errorMessage = "";

    Common.StatusCode statusCode = Common.StatusCode.OK;
    if (!request.hasCheckpointId() || request.getCheckpointId().isEmpty()) {
      LOG.info("The checkpoint id was empty, this sending empty state");
      CheckpointManager.InstanceStateCheckpoint dummyState =
          CheckpointManager.InstanceStateCheckpoint.newBuilder()
              .setCheckpointId(request.getCheckpointId())
              .setState(ByteString.EMPTY).build();

      responseBuilder.setCheckpoint(dummyState);
    } else {
      try {
        Checkpoint checkpoint = statefulStorage.restore(topologyName, request.getCheckpointId(),
                                                request.getInstance());
        LOG.info(String.format("Get checkpoint successful for checkpointId %s "
                               + "component %s taskId %d", checkpoint.getCheckpointId(),
                               checkpoint.getComponent(), checkpoint.getTaskId()));
        // Set the checkpoint-state in response
        boolean spill_disk = true;
        if (spill_disk) {
          CheckpointManager.InstanceStateCheckpoint ckpt = checkpoint.getCheckpoint();

          // spill state to local disk
          String stateURI = storeStateLocally(ckpt.getState(), ckpt.getCheckpointId());

          CheckpointManager.InstanceStateCheckpoint ckpt2 =
              CheckpointManager.InstanceStateCheckpoint.newBuilder()
                  .setStateUri(stateURI)
                  .setCheckpointId(ckpt.getCheckpointId())
                  .build();

          responseBuilder.setCheckpoint(ckpt2);
        } else {
          responseBuilder.setCheckpoint(checkpoint.getCheckpoint());
        }
      } catch (StatefulStorageException e) {
        errorMessage = String.format("Get checkpoint not successful for checkpointId %s "
                                     + "component %s taskId %d", request.getCheckpointId(),
                                     request.getInstance().getInfo().getComponentName(),
                                     request.getInstance().getInfo().getTaskId());
        LOG.log(Level.WARNING, errorMessage, e);
        statusCode = Common.StatusCode.NOTOK;
      }
    }

    responseBuilder.setStatus(Common.Status.newBuilder().setStatus(statusCode)
                              .setMessage(errorMessage));

    sendResponse(rid, channel, responseBuilder.build());
  }


  private String storeStateLocally(ByteString states, String ckptId) {
    String fileName;

    try {
      fileName = Files.createTempFile(ckptId, "state").toString();
    } catch (IOException e) {
      throw new RuntimeException("failed to create local temp file for state", e);
    }

    try (FileOutputStream fos = new FileOutputStream(new File(fileName));
         ObjectOutputStream oos = new ObjectOutputStream(fos)) {
      oos.writeObject(states);
      oos.flush();
      return fileName;
    } catch (IOException e) {
      throw new RuntimeException("failed to persist state locally", e);
    }
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

  private boolean checkRegistrationValidity(String topName, String topId) {
    return this.topologyName.equals(topName) && this.topologyId.equals(topId);
  }

  private boolean checkExistingConnection() {
    if (connection != null) {
      LOG.warning("We already have an active connection Closing it..");
      try {
        connection.close();
      } catch (IOException e) {
        LOG.log(Level.WARNING, "Failed to close connection from: "
                + connection.socket().getRemoteSocketAddress(), e);
      }
      connection = null;
      return false;
    } else {
      return true;
    }
  }
}
