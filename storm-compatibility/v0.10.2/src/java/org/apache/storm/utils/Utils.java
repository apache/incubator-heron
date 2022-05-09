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

package org.apache.storm.utils;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

import org.apache.heron.common.basics.TypeUtils;

// import org.json.simple.JSONValue;

public final class Utils {
  public static final String DEFAULT_STREAM_ID =
      org.apache.heron.api.utils.Utils.DEFAULT_STREAM_ID;

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
    return org.apache.heron.api.utils.Utils.tuple(values);
  }

  public static void sleep(long millis) {
    org.apache.heron.api.utils.Utils.sleep(millis);
  }

    /*
    public static boolean isValidConf(Map<String, Object> stormConf) {
        return normalizeConf(stormConf).equals(
          normalizeConf((Map) JSONValue.parse(JSONValue.toJSONString(stormConf))));
    }
    */

  public static Map<String, String> readCommandLineOpts() {
    return org.apache.heron.api.utils.Utils.readCommandLineOpts();
  }

    /*
    private static Object normalizeConf(Object conf) {
        if(conf==null) return new HashMap();
        if(conf instanceof Map) {
            Map confMap = new HashMap((Map) conf);
            for(Object key: confMap.keySet()) {
                Object val = confMap.get(key);
                confMap.put(key, normalizeConf(val));
            }
            return confMap;
        } else if(conf instanceof List) {
            List confList =  new ArrayList((List) conf);
            for(int i=0; i<confList.size(); i++) {
                Object val = confList.get(i);
                confList.set(i, normalizeConf(val));
            }
            return confList;
        } else if (conf instanceof Integer) {
            return ((Integer) conf).longValue();
        } else if(conf instanceof Float) {
            return ((Float) conf).doubleValue();
        } else {
            return conf;
        }
    }
    */

  public static byte[] serialize(Object obj) {
    return org.apache.heron.api.utils.Utils.serialize(obj);
  }

  public static Object deserialize(byte[] serialized) {
    return org.apache.heron.api.utils.Utils.deserialize(serialized);
  }

  public static Integer getInt(Object o) {
    return TypeUtils.getInteger(o);
  }

  public static boolean getBoolean(Object o, boolean defaultValue) {
    if (o == null) {
      return defaultValue;
    } else {
      return TypeUtils.getBoolean(o);
    }
  }

  public static byte[] toByteArray(ByteBuffer buffer) {
    return org.apache.heron.api.utils.Utils.toByteArray(buffer);
  }

  public static <S, T> T get(Map<S, T> m, S key, T defaultValue) {
    return org.apache.heron.api.utils.Utils.get(m, key, defaultValue);
  }
}
