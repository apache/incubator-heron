/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.storm.utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.net.URL;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.input.ClassLoaderObjectInputStream;
import org.apache.commons.lang.StringUtils;
import org.apache.storm.shade.org.apache.curator.ensemble.exhibitor.DefaultExhibitorRestClient;
import org.apache.storm.shade.org.apache.curator.ensemble.exhibitor.ExhibitorEnsembleProvider;
import org.apache.storm.shade.org.apache.curator.ensemble.exhibitor.Exhibitors;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.storm.shade.org.apache.curator.framework.CuratorFramework;
import org.apache.storm.Config;
import org.apache.storm.shade.org.apache.zookeeper.data.ACL;
import org.apache.storm.thrift.TBase;
import org.apache.storm.thrift.TDeserializer;
import org.apache.storm.thrift.TException;
import org.apache.storm.thrift.TSerializer;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.SafeConstructor;

import com.twitter.heron.common.basics.TypeUtils;

// import org.json.simple.JSONValue;

@SuppressWarnings({"unchecked", "rawtypes"})
public final class Utils {
  public static final String DEFAULT_STREAM_ID =
      com.twitter.heron.api.utils.Utils.DEFAULT_STREAM_ID;

  private Utils() {
  }

  public static Object newInstance(String klass) {
    try {
      Class<?> c = Class.forName(klass);
      return c.newInstance();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      throw new RuntimeException(e);
    }
  }

  public static List<Object> tuple(Object... values) {
    return com.twitter.heron.api.utils.Utils.tuple(values);
  }

  public static void sleep(long millis) {
    com.twitter.heron.api.utils.Utils.sleep(millis);
  }

    /*
    public static boolean isValidConf(Map<String, Object> stormConf) {
        return normalizeConf(stormConf).equals(
          normalizeConf((Map) JSONValue.parse(JSONValue.toJSONString(stormConf))));
    }
    */

  public static Map<String, String> readCommandLineOpts() {
    return com.twitter.heron.api.utils.Utils.readCommandLineOpts();
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
    return com.twitter.heron.api.utils.Utils.serialize(obj);
  }

  public static byte[] javaSerialize(Object obj) {
    return serialize(obj);
  }

  public static Object deserialize(byte[] serialized) {
    return com.twitter.heron.api.utils.Utils.deserialize(serialized);
  }

  private static ThreadLocal<TSerializer> threadSer = new ThreadLocal<TSerializer>();

  public static byte[] thriftSerialize(TBase t) {
    try {
      TSerializer ser = threadSer.get();
      if (ser == null) {
        ser = new TSerializer();
        threadSer.set(ser);
      }
      return ser.serialize(t);
    } catch (TException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> T thriftDeserialize(Class c, byte[] b) {
    try {
      return Utils.thriftDeserialize(c, b, 0, b.length);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static <T> T thriftDeserialize(Class c, byte[] b, int offset, int length) {
    try {
      T ret = (T) c.newInstance();
      TDeserializer des = getDes();
      des.deserialize((TBase) ret, b, offset, length);
      return ret;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static ThreadLocal<TDeserializer> threadDes = new ThreadLocal<TDeserializer>();

  private static TDeserializer getDes() {
    TDeserializer des = threadDes.get();
    if (des == null) {
      des = new TDeserializer();
      threadDes.set(des);
    }
    return des;
  }

  private static ClassLoader cl = null;

  public static <T> T javaDeserialize(byte[] serialized, Class<T> clazz) {
    try {
      ByteArrayInputStream bis = new ByteArrayInputStream(serialized);
      ObjectInputStream ois = null;
      if (null == cl) {
        ois = new ObjectInputStream(bis);
      } else {
        // Use custom class loader set in testing environment
        ois = new ClassLoaderObjectInputStream(cl, bis);
      }
      Object ret = ois.readObject();
      ois.close();
      return (T) ret;
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    } catch (ClassNotFoundException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T> String join(Iterable<T> coll, String sep) {
    Iterator<T> it = coll.iterator();
    StringBuilder ret = new StringBuilder();
    while (it.hasNext()) {
      ret.append(it.next());
      if (it.hasNext()) {
        ret.append(sep);
      }
    }
    return ret.toString();
  }

  public static CuratorFramework newCuratorStarted(Map conf, List<String> servers,
                                                   Object port, String root,
                                                   ZookeeperAuthInfo auth) {
      CuratorFramework ret = newCurator(conf, servers, port, root, auth);
      ret.start();
      return ret;
  }

  public static CuratorFramework newCuratorStarted(Map conf, List<String> servers, Object port, ZookeeperAuthInfo auth) {
    CuratorFramework ret = newCurator(conf, servers, port, auth);
    ret.start();
    return ret;
  }

  public static CuratorFramework newCurator(Map conf, List<String> servers, Object port, ZookeeperAuthInfo auth) {
    return newCurator(conf, servers, port, "", auth);
  }

  public static CuratorFramework newCurator(Map conf, List<String> servers, Object port, String root, ZookeeperAuthInfo auth) {
    List<String> serverPorts = new ArrayList<String>();
    for (String zkServer : servers) {
      serverPorts.add(zkServer + ":" + Utils.getInt(port));
    }
    String zkStr = StringUtils.join(serverPorts, ",") + root;
    CuratorFrameworkFactory.Builder builder = CuratorFrameworkFactory.builder();

    setupBuilder(builder, zkStr, conf, auth);

    return builder.build();
  }

  protected static void setupBuilder(CuratorFrameworkFactory.Builder builder, final String zkStr, Map conf, ZookeeperAuthInfo auth) {
    List<String> exhibitorServers = getStrings(conf.get(Config.STORM_EXHIBITOR_SERVERS));
    if (!exhibitorServers.isEmpty()) {
      // use exhibitor servers
      builder.ensembleProvider(new ExhibitorEnsembleProvider(
          new Exhibitors(exhibitorServers, Utils.getInt(conf.get(Config.STORM_EXHIBITOR_PORT)),
              new Exhibitors.BackupConnectionStringProvider() {
                @Override
                public String getBackupConnectionString() throws Exception {
                  // use zk servers as backup if they exist
                  return zkStr;
                }
              }),
          new DefaultExhibitorRestClient(),
          Utils.getString(conf.get(Config.STORM_EXHIBITOR_URIPATH)),
          Utils.getInt(conf.get(Config.STORM_EXHIBITOR_POLL)),
          new StormBoundedExponentialBackoffRetry(
              Utils.getInt(conf.get(Config.STORM_EXHIBITOR_RETRY_INTERVAL)),
              Utils.getInt(conf.get(Config.STORM_EXHIBITOR_RETRY_INTERVAL_CEILING)),
              Utils.getInt(conf.get(Config.STORM_EXHIBITOR_RETRY_TIMES)))));
    } else {
      builder.connectString(zkStr);
    }
    builder
        .connectionTimeoutMs(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_CONNECTION_TIMEOUT)))
        .sessionTimeoutMs(Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_SESSION_TIMEOUT)))
        .retryPolicy(new StormBoundedExponentialBackoffRetry(
            Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL)),
            Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_INTERVAL_CEILING)),
            Utils.getInt(conf.get(Config.STORM_ZOOKEEPER_RETRY_TIMES))));

    if (auth != null && auth.scheme != null && auth.payload != null) {
      builder.authorization(auth.scheme, auth.payload);
    }
  }

  public static List<String> getStrings(final Object o) {
    if (o == null) {
      return new ArrayList<String>();
    } else if (o instanceof String) {
      return new ArrayList<String>() {
        private static final long serialVersionUID = -2182685021645675803L;
        { add((String) o); }};
    } else if (o instanceof Collection) {
      List<String> answer = new ArrayList<String>();
      for (Object v : (Collection) o) {
        answer.add(v.toString());
      }
      return answer;
    } else {
      throw new IllegalArgumentException("Don't know how to convert to string list");
    }
  }

  public static String getString(Object o) {
    if (null == o) {
      throw new IllegalArgumentException("Don't know how to convert null to String");
    }
    return o.toString();
  }
  public static List<ACL> getWorkerACL(Map conf) {
      return null; //TODO: implement ACL support
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
    return com.twitter.heron.api.utils.Utils.toByteArray(buffer);
  }

  public static <S, T> T get(Map<S, T> m, S key, T defaultValue) {
    return com.twitter.heron.api.utils.Utils.get(m, key, defaultValue);
  }

  public static Map readStormConfig() {
    Map ret = readDefaultConfig();
    String confFile = System.getProperty("storm.conf.file");
    Map storm;
    if (confFile == null || confFile.equals("")) {
      storm = findAndReadConfigFile("storm.yaml", false);
    } else {
      storm = findAndReadConfigFile(confFile, true);
    }
    ret.putAll(storm);
    ret.putAll(readCommandLineOpts());
    return ret;
  }

  public static Map readDefaultConfig() {
    return findAndReadConfigFile("defaults.yaml", true);
  }

  private static Map findAndReadConfigFile(String name, boolean mustExist) {
    InputStream in = null;
    boolean confFileEmpty = false;
    try {
      in = getConfigFileInputStream(name);
      if (null != in) {
        Yaml yaml = new Yaml(new SafeConstructor());
        Map ret = (Map) yaml.load(new InputStreamReader(in));
        if (null != ret) {
          return new HashMap(ret);
        } else {
          confFileEmpty = true;
        }
      }

      if (mustExist) {
        if(confFileEmpty)
          throw new RuntimeException("Config file " + name + " doesn't have any valid storm configs");
        else
          throw new RuntimeException("Could not find config file on classpath " + name);
      } else {
          return new HashMap();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      if (null != in) {
        try {
          in.close();
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private static InputStream getConfigFileInputStream(String configFilePath)
          throws IOException {
    if (null == configFilePath) {
      throw new IOException(
              "Could not find config file, name not specified");
    }

    HashSet<URL> resources = new HashSet<>(findResources(configFilePath));
    if (resources.isEmpty()) {
      File configFile = new File(configFilePath);
      if (configFile.exists()) {
        return new FileInputStream(configFile);
      }
    } else if (resources.size() > 1) {
      throw new IOException(
              "Found multiple " + configFilePath
                      + " resources. You're probably bundling the Storm jars with your topology jar. "
                      + resources);
    } else {
//          LOG.debug("Using "+configFilePath+" from resources");
      URL resource = resources.iterator().next();
      return resource.openStream();
    }
    return null;
  }

  private static List<URL> findResources(String name) {
    try {
      Enumeration<URL> resources = Thread.currentThread().getContextClassLoader().getResources(name);
      List<URL> ret = new ArrayList<>();
      while (resources.hasMoreElements()) {
        ret.add(resources.nextElement());
      }
      return ret;
    } catch(IOException e) {
      throw new RuntimeException(e);
    }
  }
}
