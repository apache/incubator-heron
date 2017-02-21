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

package backtype.storm.serialization;

import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;

import backtype.storm.Config;


public class DefaultKryoFactory implements IKryoFactory {

  @Override
  @SuppressWarnings("rawtypes")
  public Kryo getKryo(Map conf) {
    KryoSerializableDefault k = new KryoSerializableDefault();
    k.setRegistrationRequired(
        !((Boolean) conf.get(Config.TOPOLOGY_FALL_BACK_ON_JAVA_SERIALIZATION)));
    k.setReferences(false);
    return k;
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void preRegister(Kryo k, Map conf) {
  }

  @SuppressWarnings("rawtypes")
  public void postRegister(Kryo k, Map conf) {
    ((KryoSerializableDefault) k).overrideDefault(true);
  }

  @Override
  @SuppressWarnings("rawtypes")
  public void postDecorate(Kryo k, Map conf) {
  }

  public static class KryoSerializableDefault extends Kryo {
    private boolean override = false;

    public void overrideDefault(boolean value) {
      override = value;
    }

    @Override
    @SuppressWarnings("rawtypes") // superclass doesn't use types
    public Serializer getDefaultSerializer(Class type) {
      if (override) {
        return new SerializableSerializer();
      } else {
        return super.getDefaultSerializer(type);
      }
    }
  }
}
