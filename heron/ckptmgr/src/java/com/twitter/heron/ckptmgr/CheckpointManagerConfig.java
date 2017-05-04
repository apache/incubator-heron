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

import java.util.HashMap;
import java.util.Map;

import com.twitter.heron.common.config.ConfigReader;

public class CheckpointManagerConfig {
  public static final String STORAGE_CONFIG = "heron.statefulstorage.config";
  public static final String STORAGE_CLASSNAME = "heron.class.statefulstorage";
  public static final String WRITE_BATCH_SIZE = "heron.ckptmgr.network.write.batch.size.bytes";
  public static final String WRITE_BATCH_TIME = "heron.ckptmgr.network.write.batch.time.ms";
  public static final String READ_BATCH_SIZE = "heron.ckptmgr.network.read.batch.size.bytes";
  public static final String READ_BATCH_TIME = "heron.ckptmgr.network.read.batch.time.ms";
  public static final String SOCKET_SEND_SIZE =
                             "heron.ckptmgr.network.options.socket.send.buffer.size.bytes";
  public static final String SOCKET_RECEIVE_SIZE =
                             "heron.ckptmgr.network.options.socket.receive.buffer.size.bytes";

  private final Map<String, Object> backendConfig = new HashMap<>();
  private final Map<String, Object> ckptmgrConfig = new HashMap<>();

  @SuppressWarnings("unchecked")
  public CheckpointManagerConfig(String filename) {
    ckptmgrConfig.putAll(ConfigReader.loadFile(filename));
    String backend = (String) ckptmgrConfig.get(STORAGE_CONFIG);
    backendConfig.putAll((Map<String, Object>) ckptmgrConfig.get(backend));
  }

  public Object getBackendConfig(String key) {
    return backendConfig.get(key);
  }

  public Object getCkptMgrConfig(String key) {
    return ckptmgrConfig.get(key);
  }

  public Map<String, Object> getBackendConfig() {
    return backendConfig;
  }

  public long getWriteBatchSizeBytes() {
    return (long) ckptmgrConfig.get(WRITE_BATCH_SIZE);
  }

  public long getWriteBatchSizeMs() {
    return (long) ckptmgrConfig.get(WRITE_BATCH_TIME);
  }

  public long getReadBatchSizeBytes() {
    return (long) ckptmgrConfig.get(READ_BATCH_SIZE);
  }

  public long getReadBatchSizeMs() {
    return (long) ckptmgrConfig.get(READ_BATCH_TIME);
  }

  public int getSocketSendSize() {
    return (int) ckptmgrConfig.get(SOCKET_SEND_SIZE);
  }

  public int getSocketReceiveSize() {
    return (int) ckptmgrConfig.get(SOCKET_RECEIVE_SIZE);
  }
}
