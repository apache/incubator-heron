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

package org.apache.heron.api.serializer;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;

import org.apache.heron.api.Config;
import org.apache.heron.api.tuple.Values;
import org.apache.heron.api.utils.Utils;

/**
 * KryoSerializer is a wrapper around Heron's IPluggableSerializer.
 * Streamlet based topologies turning on kryo serialization are based off of it.
 */
public class KryoSerializer implements IPluggableSerializer {
  private static final Logger LOG = Logger.getLogger(KryoSerializer.class.getName());
  private static final String DEFAULT_FACTORY =
      "org.apache.heron.api.serializer.DefaultKryoFactory";

  private IKryoFactory kryoFactory;
  private Kryo kryo;
  private Output kryoOut;
  private Input kryoIn;

  @Override
  public void initialize(Map<String, Object> config) {
    String factoryClassName =
        (String) config.getOrDefault(Config.TOPOLOGY_KRYO_FACTORY, DEFAULT_FACTORY);
    kryoFactory = (IKryoFactory) Utils.newInstance(factoryClassName);
    kryo = kryoFactory.getKryo(config);
    kryoOut = new Output(2000, 2000000000);
    kryoIn = new Input(1);

    boolean skipMissing =
        (boolean) config.getOrDefault(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS, false);
    registerDefaultSerializers(kryo);
    kryoFactory.preRegister(kryo, config);
    registerUserSerializers(kryo, config, skipMissing);
    kryoFactory.postRegister(kryo, config);

    applyUserDecorators(kryo, config, skipMissing);
    kryoFactory.postDecorate(kryo, config);
  }

  @Override
  public byte[] serialize(Object object) {
    kryoOut.reset();
    kryo.writeClassAndObject(kryoOut, object);
    return kryoOut.toBytes();
  }

  @Override
  public Object deserialize(byte[] input) {
    kryoIn.setBuffer(input);
    return kryo.readClassAndObject(kryoIn);
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static void registerDefaultSerializers(Kryo k) {
    // Default serializers
    k.register(byte[].class);
    k.register(ArrayList.class);
    k.register(HashMap.class);
    k.register(HashSet.class);
    k.register(BigInteger.class, new DefaultSerializers.BigIntegerSerializer());
    k.register(Values.class);
  }

  private static void registerUserSerializers(
        Kryo k, Map<String, Object> config, boolean skipMissing) {
    // Configured serializers
    Map<String, String> registrations = normalizeKryoRegister(config);

    for (String klassName : registrations.keySet()) {
      String serializerClassName = registrations.get(klassName);
      try {
        Class klass = Class.forName(klassName);
        Class serializerClass = null;
        if (serializerClassName != null) {
          serializerClass = Class.forName(serializerClassName);
        }
        LOG.info("Doing kryo.register for class " + klass);
        if (serializerClass == null) {
          k.register(klass);
        } else {
          k.register(klass, resolveSerializerInstance(k, klass, serializerClass));
        }

      } catch (ClassNotFoundException e) {
        if (skipMissing) {
          LOG.info("Could not find serialization or class for "
              + serializerClassName + ". Skipping registration...");
        } else {
          throw new RuntimeException(e);
        }
      }
    }
  }

  private static void applyUserDecorators(
        Kryo k, Map<String, Object> config, boolean skipMissing) {
    if (config.containsKey(Config.TOPOLOGY_KRYO_DECORATORS)) {
      for (String klassName : (List<String>) config.get(Config.TOPOLOGY_KRYO_DECORATORS)) {
        try {
          Class klass = Class.forName(klassName);
          IKryoDecorator decorator = (IKryoDecorator) klass.newInstance();
          decorator.decorate(k);
        } catch (ClassNotFoundException e) {
          if (skipMissing) {
            LOG.info("Could not find kryo decorator named "
                + klassName + ". Skipping registration...");
          } else {
            throw new RuntimeException(e);
          }
        } catch (InstantiationException e) {
          throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
          throw new RuntimeException(e);
        }
      }
    }
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Map<String, String> normalizeKryoRegister(Map<String, Object> config) {
    // TODO: de-duplicate this logic with the code in nimbus
    Object res = config.get(Config.TOPOLOGY_KRYO_REGISTER);
    if (res == null) {
      return new TreeMap<String, String>();
    }
    Map<String, String> ret = new HashMap<>();
    // Register config is a list. Each value can be either a String or a map
    for (Object o: (List) res) {
      if (o instanceof Map) {
        ret.putAll((Map) o);
      } else {
        ret.put((String) o, null);
      }
    }

    //ensure always same order for registrations with TreeMap
    return new TreeMap<String, String>(ret);
  }


  @SuppressWarnings("rawtypes")
  private static Serializer resolveSerializerInstance(
      Kryo k, Class superClass, Class<? extends Serializer> serializerClass) {
    Constructor<? extends Serializer> ctor;

    try {
      ctor = serializerClass.getConstructor(Kryo.class, Class.class);
      return ctor.newInstance(k, superClass);
    } catch (NoSuchMethodException | InvocationTargetException
        | InstantiationException | IllegalAccessException ex) {
      // do nothing
    }

    try {
      ctor = serializerClass.getConstructor(Kryo.class);
      return ctor.newInstance(k);
    } catch (NoSuchMethodException | InvocationTargetException
        | InstantiationException | IllegalAccessException ex) {
      // do nothing
    }

    try {
      ctor = serializerClass.getConstructor(Class.class);
      return ctor.newInstance(k);
    } catch (NoSuchMethodException | InvocationTargetException
        | InstantiationException | IllegalAccessException ex) {
      // do nothing
    }

    try {
      return serializerClass.newInstance();
    } catch (InstantiationException | IllegalAccessException ex) {
      // do nothing
    }

    throw new IllegalArgumentException(
        String.format("Unable to create serializer \"%s\" for class: %s",
            serializerClass.getName(), superClass.getName()));
  }
}
