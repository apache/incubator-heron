/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.storm.kafka;

import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryNTimes;
import org.apache.storm.Config;
import org.apache.storm.utils.Utils;
import org.apache.zookeeper.CreateMode;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ZkState {
  private static final Logger LOG = LoggerFactory.getLogger(ZkState.class);
  private CuratorFramework curator;

  @SuppressWarnings("unchecked")
  private CuratorFramework newCurator(Map<String, Object> stateConf) throws Exception {
    Integer port = (Integer) stateConf.get(Config.TRANSACTIONAL_ZOOKEEPER_PORT);
    String serverPorts = "";
    for (String server : (List<String>) stateConf.get(Config.TRANSACTIONAL_ZOOKEEPER_SERVERS)) {
      serverPorts = serverPorts + server + ":" + port + ",";
    }
    return CuratorFrameworkFactory.newClient(serverPorts,
        Utils.getInt(stateConf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)),
        Utils.getInt(stateConf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)),
        new RetryNTimes(Utils.getInt(stateConf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES)),
            Utils.getInt(stateConf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL))));
  }

  public CuratorFramework getCurator() {
    assert curator != null;
    return curator;
  }

  public ZkState(Map<String, Object> stateConf) {
    try {
      curator = newCurator(stateConf);
      curator.start();
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void writeJSON(String path, Map<Object, Object> data) {
    LOG.debug("Writing {} the data {}", path, data.toString());
    writeBytes(path, JSONValue.toJSONString(data).getBytes(Charset.forName("UTF-8")));
  }

  public void writeBytes(String path, byte[] bytes) {
    try {
      if (curator.checkExists().forPath(path) == null) {
        curator.create()
            .creatingParentsIfNeeded()
            .withMode(CreateMode.PERSISTENT)
            .forPath(path, bytes);
      } else {
        curator.setData().forPath(path, bytes);
      }
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("unchecked")
  public Map<Object, Object> readJSON(String path) {
    try {
      byte[] b = readBytes(path);
      if (b == null) {
        return null;
      }
      return (Map<Object, Object>) JSONValue.parse(new String(b, "UTF-8"));
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] readBytes(String path) {
    try {
      if (curator.checkExists().forPath(path) != null) {
        return curator.getData().forPath(path);
      } else {
        return null;
      }
      // SUPPRESS CHECKSTYLE IllegalCatch
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    curator.close();
    curator = null;
  }
}
