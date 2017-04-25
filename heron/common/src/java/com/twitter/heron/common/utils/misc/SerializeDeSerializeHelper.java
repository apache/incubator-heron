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

package com.twitter.heron.common.utils.misc;

import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.twitter.heron.api.Config;
import com.twitter.heron.api.serializer.IPluggableSerializer;
import com.twitter.heron.api.serializer.JavaSerializer;

/**
 * Get the serializer according to the serializerClassName
 */
public final class SerializeDeSerializeHelper {
  private static final Logger LOG = Logger.getLogger(SerializeDeSerializeHelper.class.getName());

  private SerializeDeSerializeHelper() {
  }

  public static IPluggableSerializer getSerializer(Map<String, Object> config) {
    IPluggableSerializer serializer;
    try {
      String serializerClassName = (String) config.get(Config.TOPOLOGY_SERIALIZER_CLASSNAME);
      if (serializerClassName == null) {
        LOG.log(Level.WARNING, "Serializer class name not provided. "
            + "Fall back to Java serializer. "
            + "This could cause serious performance degradation. "
            + "You can specify to use Kryo as serializer. "
            + "See https://twitter.github.io/heron/docs/developers/serialization/ for details");
        serializer = new JavaSerializer();
      } else {
        serializer = (IPluggableSerializer) Class.forName(serializerClassName).newInstance();
      }
      serializer.initialize(config);
      return serializer;
    } catch (ClassNotFoundException ex) {
      throw new RuntimeException("Serializer class must be in class path " + ex);
    } catch (InstantiationException ex) {
      throw new RuntimeException(
          "Serializer class must be concrete and have a nullary constructor " + ex);
    } catch (IllegalAccessException ex) {
      throw new RuntimeException("Serializer class constructor must be public " + ex);
    }
  }
}
