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

package org.apache.heron.statefulstorage.dlog;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;
import java.util.logging.Logger;

import org.apache.distributedlog.AppendOnlyStreamWriter;
import org.apache.distributedlog.DistributedLogConfiguration;
import org.apache.distributedlog.api.DistributedLogManager;
import org.apache.distributedlog.api.namespace.Namespace;
import org.apache.distributedlog.api.namespace.NamespaceBuilder;
import org.apache.heron.common.basics.SysUtils;
import org.apache.heron.dlog.DLInputStream;
import org.apache.heron.dlog.DLOutputStream;
import org.apache.heron.proto.ckptmgr.CheckpointManager;
import org.apache.heron.proto.system.PhysicalPlans;
import org.apache.heron.spi.statefulstorage.Checkpoint;
import org.apache.heron.spi.statefulstorage.IStatefulStorage;
import org.apache.heron.spi.statefulstorage.StatefulStorageException;

public class DlogStorage implements IStatefulStorage {

  private static final Logger LOG = Logger.getLogger(DlogStorage.class.getName());

  public static final String NS_URI_KEY = "heron.statefulstorage.dlog.namespace.uri";
  public static final String NUM_REPLICAS_KEY = "heron.statefulstorage.dlog.num.replicas";

  private String checkpointNamespaceUriStr;
  private URI checkpointNamespaceUri;
  private int numReplicas = 3;

  // the namespace instance
  private final Supplier<NamespaceBuilder> nsBuilderSupplier;
  private Namespace namespace;

  public DlogStorage() {
    this(() -> NamespaceBuilder.newBuilder());
  }

  public DlogStorage(Supplier<NamespaceBuilder> nsBuilderSupplier) {
    this.nsBuilderSupplier = nsBuilderSupplier;
  }

  @Override
  public void init(Map<String, Object> conf) throws StatefulStorageException {

    LOG.info("Initializing ... Config: " + conf.toString());
    LOG.info("Class path: " + System.getProperty("java.class.path"));

    checkpointNamespaceUriStr = (String) conf.get(NS_URI_KEY);
    checkpointNamespaceUri = URI.create(checkpointNamespaceUriStr);
    Integer numReplicasValue = (Integer) conf.get(NUM_REPLICAS_KEY);
    this.numReplicas = null == numReplicasValue ? 3 : numReplicasValue;

    try {
      this.namespace = initializeNamespace(checkpointNamespaceUri);
    } catch (IOException ioe) {
      throw new StatefulStorageException("Failed to open distributedlog namespace @ "
          + checkpointNamespaceUri, ioe);
    }
  }

  Namespace initializeNamespace(URI uri) throws IOException {
    DistributedLogConfiguration conf = new DistributedLogConfiguration()
        .setWriteLockEnabled(false)
        .setOutputBufferSize(256 * 1024)                  // 256k
        .setPeriodicFlushFrequencyMilliSeconds(0)         // disable periodical flush
        .setImmediateFlushEnabled(false)                  // disable immediate flush
        .setLogSegmentRollingIntervalMinutes(0)           // disable time-based rolling
        .setMaxLogSegmentBytes(Long.MAX_VALUE)            // disable size-based rolling
        .setExplicitTruncationByApplication(true)         // no auto-truncation
        .setRetentionPeriodHours(Integer.MAX_VALUE)       // long retention
        .setEnsembleSize(numReplicas)                     // replica settings
        .setWriteQuorumSize(numReplicas)
        .setAckQuorumSize(numReplicas)
        .setUseDaemonThread(true)                         // use daemon thread
        .setNumWorkerThreads(1)                           // use 1 worker thread
        .setBKClientNumberIOThreads(1);

    return this.nsBuilderSupplier.get()
        .clientId("heron-stateful-storage")
        .conf(conf)
        .uri(uri)
        .build();
  }

  protected OutputStream openOutputStream(String path) throws IOException {
    DistributedLogManager dlm = namespace.openLog(path);
    AppendOnlyStreamWriter writer = dlm.getAppendOnlyStreamWriter();
    return new DLOutputStream(dlm, writer);
  }

  protected InputStream openInputStream(String logName)
      throws IOException {
    DistributedLogManager dlm = namespace.openLog(logName);
    return new DLInputStream(dlm);
  }

  @Override
  public void close() {
    if (null != namespace) {
      namespace.close();
    }
  }

  @Override
  public void store(Checkpoint checkpoint) throws StatefulStorageException {
    String checkpointPath = getCheckpointPath(
        checkpoint.getTopologyName(),
        checkpoint.getCheckpointId(),
        checkpoint.getComponent(),
        checkpoint.getTaskId());

    OutputStream out = null;
    try {
      out = openOutputStream(checkpointPath);
      checkpoint.getCheckpoint().writeTo(out);
    } catch (IOException e) {
      throw new StatefulStorageException("Failed to persist checkpoint @ " + checkpointPath, e);
    } finally {
      SysUtils.closeIgnoringExceptions(out);
    }
  }

  @Override
  public Checkpoint restore(String topologyName,
                            String checkpointId,
                            PhysicalPlans.Instance instanceInfo)
      throws StatefulStorageException {
    String checkpointPath = getCheckpointPath(
        topologyName,
        checkpointId,
        instanceInfo.getInfo().getComponentName(),
        instanceInfo.getInfo().getTaskId());

    InputStream in = null;
    CheckpointManager.InstanceStateCheckpoint state;
    try {
      in = openInputStream(checkpointPath);
      state = CheckpointManager.InstanceStateCheckpoint.parseFrom(in);
    } catch (IOException ioe) {
      throw new StatefulStorageException("Failed to read checkpoint from " + checkpointPath, ioe);
    } finally {
      SysUtils.closeIgnoringExceptions(in);
    }

    return new Checkpoint(topologyName, instanceInfo, state);
  }

  @Override
  public void dispose(String topologyName,
                      String oldestCheckpointId,
                      boolean deleteAll)
      throws StatefulStorageException {

    // Currently dlog doesn't support recursive deletion. so we have to fetch all the checkpoints
    // and delete individual checkpoints.
    // TODO (sijie): replace the logic here once distributedlog supports recursive deletion.

    String topologyCheckpointRoot = getTopologyCheckpointRoot(topologyName);
    URI topologyUri = URI.create(checkpointNamespaceUriStr + topologyCheckpointRoot);
    // get checkpoints
    Namespace topologyNs = null;
    Iterator<String> checkpoints;
    try {
      topologyNs = initializeNamespace(topologyUri);
      checkpoints = topologyNs.getLogs();
    } catch (IOException ioe) {
      throw new StatefulStorageException("Failed to open topology namespace", ioe);
    } finally {
      if (null != topologyNs) {
        topologyNs.close();
      }
    }

    while (checkpoints.hasNext()) {
      String checkpointId = checkpoints.next();
      if (deleteAll || checkpointId.compareTo(oldestCheckpointId) < 0) {
        URI checkpointUri =
            URI.create(checkpointNamespaceUriStr + topologyCheckpointRoot + "/" + checkpointId);
        try {
          deleteCheckpoint(checkpointUri);
        } catch (IOException e) {
          throw new StatefulStorageException("Failed to remove checkpoint "
              + checkpointId + " for topology " + topologyName, e);
        }
      }
    }
  }

  private void deleteCheckpoint(URI checkpointUri) throws IOException {
    Namespace checkpointNs = initializeNamespace(checkpointUri);
    try {
      Iterator<String> checkpoints = checkpointNs.getLogs();
      while (checkpoints.hasNext()) {
        String checkpoint = checkpoints.next();
        checkpointNs.deleteLog(checkpoint);
      }
    } finally {
      checkpointNs.close();
    }
  }


  private static String getTopologyCheckpointRoot(String topologyName) {
    return String.format("/%s", topologyName);
  }

  private static String getCheckpointDir(String topologyName,
                                         String checkpointId,
                                         String componentName) {
    return String.format("%s/%s/%s",
        getTopologyCheckpointRoot(topologyName), checkpointId, componentName);
  }

  private static String getCheckpointPath(String topologyName,
                                          String checkpointId,
                                          String componentName,
                                          int taskId) {
    return String.format("%s_%d", getCheckpointDir(topologyName, checkpointId, componentName),
                         taskId);
  }
}
