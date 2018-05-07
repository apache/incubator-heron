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


package org.apache.heron.streamlet.impl;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.CollectionSerializer;
import com.esotericsoftware.kryo.serializers.DefaultSerializers;
import com.esotericsoftware.kryo.serializers.MapSerializer;

import org.apache.heron.api.serializer.IPluggableSerializer;

/**
 * KryoSerializer is a wrapper around Heron's IPluggableSerializer.
 * Streamlet based topologies turning on kryo serialization are based off of it.
 */
public class KryoSerializer implements IPluggableSerializer {
  private Kryo kryo;
  private Output kryoOut;
  private Input kryoIn;

  /**
   * A quick utility function that determines whether kryo has been linked
   * with the streamlet binary
   */
  public static void checkForKryo() {
    Kryo k = new Kryo();
  }

  @Override
  public void initialize(Map<String, Object> config) {
    kryo = getKryo();
    kryoOut = new Output(2000, 2000000000);
    kryoIn = new Input(1);
  }

  @Override
  public byte[] serialize(Object object) {
    kryoOut.clear();
    kryo.writeClassAndObject(kryoOut, object);
    return kryoOut.toBytes();
  }

  @Override
  public Object deserialize(byte[] input) {
    kryoIn.setBuffer(input);
    return kryo.readClassAndObject(kryoIn);
  }

  private Kryo getKryo() {
    Kryo k = new Kryo();
    k.setRegistrationRequired(false);
    k.setReferences(false);
    k.register(byte[].class);
    k.register(ArrayList.class, new ArrayListSerializer());
    k.register(HashMap.class, new HashMapSerializer());
    k.register(HashSet.class, new HashSetSerializer());
    k.register(BigInteger.class, new DefaultSerializers.BigIntegerSerializer());
    return k;
  }

  private class ArrayListSerializer extends CollectionSerializer {
    @Override
    @SuppressWarnings("rawtypes") // extending Kryo class that uses raw types
    public Collection create(Kryo k, Input input, Class<Collection> type) {
      return new ArrayList();
    }
  }

  private class HashMapSerializer extends MapSerializer {
    @Override
    @SuppressWarnings("rawtypes") // extending kryo class signature that takes Map
    public Map<String, Object> create(Kryo k, Input input, Class<Map> type) {
      return new HashMap<>();
    }
  }

  private class HashSetSerializer extends CollectionSerializer {
    @Override
    @SuppressWarnings("rawtypes") // extending Kryo class that uses raw types
    public Collection create(Kryo k, Input input, Class<Collection> type) {
      return new HashSet();
    }
  }
}
