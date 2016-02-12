package org.apache.storm.serialization;

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
import com.esotericsoftware.kryo.serializers.DefaultSerializers.BigIntegerSerializer;

import org.apache.storm.Config;
import org.apache.storm.serialization.types.ArrayListSerializer;
import org.apache.storm.serialization.types.HashMapSerializer;
import org.apache.storm.serialization.types.HashSetSerializer;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.ListDelegate;
import org.apache.storm.utils.Utils;

public class SerializationFactory {
  public static final Logger LOG = Logger.getLogger(SerializationFactory.class.getName());

  public static Kryo getKryo(Map conf) {
    IKryoFactory kryoFactory = (IKryoFactory) Utils.newInstance((String) conf.get(Config.TOPOLOGY_KRYO_FACTORY));
    Kryo k = kryoFactory.getKryo(conf);
    k.register(byte[].class);
    k.register(ListDelegate.class);
    k.register(ArrayList.class, new ArrayListSerializer());
    k.register(HashMap.class, new HashMapSerializer());
    k.register(HashSet.class, new HashSetSerializer());
    k.register(BigInteger.class, new BigIntegerSerializer());
    // k.register(TransactionAttempt.class);
    k.register(Values.class);
    // k.register(org.apache.storm.metric.api.IMetricsConsumer.DataPoint.class);
    // k.register(org.apache.storm.metric.api.IMetricsConsumer.TaskInfo.class);
        /*
        try {
            JavaBridge.registerPrimitives(k);
            JavaBridge.registerCollections(k);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }
        */

    Map<String, String> registrations = normalizeKryoRegister(conf);

    kryoFactory.preRegister(k, conf);

    boolean skipMissing = (Boolean) conf.get(Config.TOPOLOGY_SKIP_MISSING_KRYO_REGISTRATIONS);
    for (String klassName : registrations.keySet()) {
      String serializerClassName = registrations.get(klassName);
      try {
        Class klass = Class.forName(klassName);
        Class serializerClass = null;
        if (serializerClassName != null)
          serializerClass = Class.forName(serializerClassName);
        LOG.info("Doing kryo.register for class " + klass);
        if (serializerClass == null) {
          k.register(klass);
        } else {
          k.register(klass, resolveSerializerInstance(k, klass, serializerClass));
        }

      } catch (ClassNotFoundException e) {
        if (skipMissing) {
          LOG.info("Could not find serialization or class for " + serializerClassName + ". Skipping registration...");
        } else {
          throw new RuntimeException(e);
        }
      }
    }

    kryoFactory.postRegister(k, conf);

    if (conf.get(Config.TOPOLOGY_KRYO_DECORATORS) != null) {
      for (String klassName : (List<String>) conf.get(Config.TOPOLOGY_KRYO_DECORATORS)) {
        try {
          Class klass = Class.forName(klassName);
          IKryoDecorator decorator = (IKryoDecorator) klass.newInstance();
          decorator.decorate(k);
        } catch (ClassNotFoundException e) {
          if (skipMissing) {
            LOG.info("Could not find kryo decorator named " + klassName + ". Skipping registration...");
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

    kryoFactory.postDecorate(k, conf);

    return k;
  }

  private static Serializer resolveSerializerInstance(Kryo k, Class superClass, Class<? extends Serializer> serializerClass) {
    try {
      try {
        return serializerClass.getConstructor(Kryo.class, Class.class).newInstance(k, superClass);
      } catch (Exception ex1) {
        try {
          return serializerClass.getConstructor(Kryo.class).newInstance(k);
        } catch (Exception ex2) {
          try {
            return serializerClass.getConstructor(Class.class).newInstance(superClass);
          } catch (Exception ex3) {
            return serializerClass.newInstance();
          }
        }
      }
    } catch (Exception ex) {
      throw new IllegalArgumentException("Unable to create serializer \""
          + serializerClass.getName()
          + "\" for class: "
          + superClass.getName(), ex);
    }
  }

  private static Map<String, String> normalizeKryoRegister(Map conf) {
    // TODO: de-duplicate this logic with the code in nimbus
    Object res = conf.get(Config.TOPOLOGY_KRYO_REGISTER);
    if (res == null) return new TreeMap<String, String>();
    Map<String, String> ret = new HashMap<String, String>();
    if (res instanceof Map) {
      ret = (Map<String, String>) res;
    } else {
      for (Object o : (List) res) {
        if (o instanceof Map) {
          ret.putAll((Map) o);
        } else {
          ret.put((String) o, null);
        }
      }
    }

    //ensure always same order for registrations with TreeMap
    return new TreeMap<String, String>(ret);
  }
}
