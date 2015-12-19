package com.twitter.heron.common.utils.misc;

import java.util.Map;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.api.serializer.KryoSerializer;

/**
 * Get the serializer according to the serializerClassName
 */
public class SerializeDeSerializeHelper {
  public static IPluggableSerializer getSerializer(Map config) {
    IPluggableSerializer serializer;
    try {
      String serializerClassName = (String) config.get(Config.TOPOLOGY_SERIALIZER_CLASSNAME);
      if (serializerClassName == null) {
        serializer = new KryoSerializer();
      } else {
        serializer = (IPluggableSerializer) Class.forName(serializerClassName).newInstance();
      }
      serializer.initialize(config);
      return serializer;
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException(ex + " Serializer class must be in class path.");
    } catch (InstantiationException ex) {
      throw new RuntimeException(ex + " Serializer class must be concrete and have a nullary constructor");
    } catch (IllegalAccessException ex) {
      throw new RuntimeException(ex + " Serializer class constructor must be public");
    }
  }
}
