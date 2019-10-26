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

package org.apache.heron.api.utils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import com.google.protobuf.ByteString;

import org.apache.heron.api.Config;
import org.apache.heron.api.generated.TopologyAPI;

public final class Utils {
  private static final Logger LOG = Logger.getLogger(Utils.class.getName());

  public static final String DEFAULT_STREAM_ID = "default";

  private Utils() {
  }

  public static Object newInstance(String klass) {
    try {
      Class<?> c = Class.forName(klass);
      return c.newInstance();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException("Failed to create instance for class: " + klass, e);
    }
  }

  public static List<Object> tuple(Object... values) {
    List<Object> ret = new ArrayList<>();
    Collections.addAll(ret, values);
    return ret;
  }

  public static void sleep(long millis) {
    try {
      Thread.sleep(millis);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }

  public static Map<String, String> readCommandLineOpts() {
    Map<String, String> ret = new HashMap<>();
    String commandOptions = System.getenv("HERON_OPTIONS");
    if (commandOptions != null) {
      commandOptions = commandOptions.replaceAll("%%%%", " ");
      String[] configs = commandOptions.split(",");
      for (String config : configs) {
        String[] options = config.split("=");
        if (options.length == 2) {
          ret.put(options[0], options[1]);
        }
      }
    }
    return ret;
  }

  public static byte[] serialize(Object obj) {
    try {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      ObjectOutputStream oos = new ObjectOutputStream(bos);
      oos.writeObject(obj);
      oos.close();
      return bos.toByteArray();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  public static Object deserialize(byte[] serialized) {
    try {
      ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
      ObjectInputStream ois = new ObjectInputStream(bis);
      Object ret = ois.readObject();
      ois.close();
      return ret;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static byte[] toByteArray(ByteBuffer buffer) {
    byte[] ret = new byte[buffer.remaining()];
    buffer.get(ret, 0, ret.length);
    return ret;
  }

  public static <S, T> T get(Map<S, T> m, S key, T defaultValue) {
    T ret = m.get(key);
    if (ret == null) {
      ret = defaultValue;
    }

    return ret;
  }

  /**
   * Converts a Heron Config object into a TopologyAPI.Config.Builder. Config entries with null
   * keys or values are ignored.
   *
   * @param config heron Config object
   * @return TopologyAPI.Config.Builder with values loaded from config
   */
  public static TopologyAPI.Config.Builder getConfigBuilder(Config config) {
    TopologyAPI.Config.Builder cBldr = TopologyAPI.Config.newBuilder();
    Set<String> apiVars = config.getApiVars();
    for (String key : config.keySet()) {
      if (key == null) {
        LOG.warning("ignore: null config key found");
        continue;
      }
      Object value = config.get(key);
      if (value == null) {
        LOG.warning("ignore: config key " + key + " has null value");
        continue;
      }
      TopologyAPI.Config.KeyValue.Builder b = TopologyAPI.Config.KeyValue.newBuilder();
      b.setKey(key);
      if (apiVars.contains(key)) {
        b.setType(TopologyAPI.ConfigValueType.STRING_VALUE);
        b.setValue(value.toString());
      } else {
        b.setType(TopologyAPI.ConfigValueType.JAVA_SERIALIZED_VALUE);
        b.setSerializedValue(ByteString.copyFrom(serialize(value)));
      }
      cBldr.addKvs(b);
    }

    return cBldr;
  }

  public static double zeroIfNaNOrInf(double x) {
    return (Double.isNaN(x) || Double.isInfinite(x)) ? 0.0 : x;
  }

  public static Integer assignKeyToTask(int key, List<Integer> taskIds) {
    int index = key % taskIds.size();
    // Make sure index is not negative
    index = index >= 0 ? index : index + taskIds.size();
    return taskIds.get(index);
  }
}
